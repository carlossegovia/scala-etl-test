import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.Map

case class ProductDetail(product_name: String, aisle: String, department: String)

case class Product(order_id: Long, user_id: Long, order_number: Int, order_dow: Int, order_hour_of_day: Int,
                   days_since_prior_order: Float, order_detail: String)


object applaudo_etl {

  val SasKey = "?sv=2019-12-12&ss=bfqt&srt=sco&sp=rlx&se=2030-07-28T18:45:41Z&st=2020-07-27T10:45:41Z&spr=https" +
    "&sig" +
    "=cJncLH0UHtfEK1txVC2BNCAwJqvcBrAt5QS2XeL9bUE%3D"
  val ContainerName = "ordersdow"
  val StorageAccountName = "orderstg"
  val ProductSchema = Encoders.product[Product].schema
  val BqBucket = "test-bucket-concrete-flare-312721"


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("AppaludoETL").getOrCreate()
    spark.conf.set(s"fs.azure.sas.$ContainerName.$StorageAccountName.blob.core.windows.net", SasKey)
    spark.conf.set("temporaryGcsBucket", BqBucket)

    // Merge Product's data from Azure Blob Storage and SQL Server
    val dfProducts = mergeAndTransformProductData(getDataFromBlobStorage(spark), getDataFromSQLServer(spark))

    // Get Product's Detail data from API
    val dfProductDetails = getDataFromAPI(spark).withColumnRenamed("aisle", "aisle_pd")

    // Join Product's and Product's Detail data
    val dfProductJoined = dfProducts.join(broadcast(dfProductDetails), dfProducts("product") === dfProductDetails
    ("product_name"), "left").drop("aisle_pd", "product_name")

    // Clean and validate data values, then write into a BigQuery table
    val dfValidated = validateDataValues(dfProductJoined)

    dfValidated.write.mode("overwrite").format("bigquery")
      .option("table", "test.products")
      .save()

    // Create the table with the Clients Category and Segmentation
    val dfCategory = getClientsCategory(dfValidated)
    val dfSegmentation = getClientsSegmentation(dfValidated)
    val dfClients = dfCategory.join(dfSegmentation, Seq("user_id"))

    dfClients.write.mode("overwrite").format("bigquery")
      .option("table", "test.clients")
      .save()

  }

  /**
    * Fetch Products data from Azure Blob Storage and return it in a DataFrame.
    *
    */
  def getDataFromBlobStorage(spark: SparkSession): DataFrame = {
    val path = s"wasbs://$ContainerName@$StorageAccountName.blob.core.windows.net"
    spark.read.schema(ProductSchema)
      .option("header", "false")
      .option("escape", "\"")
      .option("mode", "DROPMALFORMED").csv(path + "/0*.csv")

  }

  /**
    * Fetch Products data from a SQL Server instance and return it in a DataFrame.
    *
    */
  def getDataFromSQLServer(spark: SparkSession): DataFrame = {
    val df = spark.read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://orderservers.database.windows.net;database=orderdb")
      .option("dbtable", "dbo.order_details")
      .option("user", "etlguest")
      .option("password", "Etltest_2020")
      .load()

    ProductSchema.fields.foldLeft(df) {
      (df, s) => df.withColumn(s.name, df(s.name).cast(s.dataType))
    }
  }

  /**
    * Fetch Products Detail data from an API and return it in a DataFrame.
    *
    *
    */
  def getDataFromAPI(spark: SparkSession): DataFrame = {
    val url = "https://etlwebapp.azurewebsites.net/api/products"
    implicit val formats = DefaultFormats
    val data = List(scala.io.Source.fromURL(url).mkString)(0)
    val elements = (parse(data) \\ "results" \\ "items").children.map(_.extract[ProductDetail])
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    elements.toDF()
  }

  /**
    * Make an union to merge the two given DataFrames and clean the fields with unexpected (bad) format. Also,
    * split and explode the original `order_detail` column in multiples ones to accomplish the desired requirements.
    * Notes:
    * Cleaned the `product` column that has non-ascii characters
    * Casted the `days_since_prior_order` column from Double to Int
    * Transformed unexpected data from the `order_hour_of_day` column
    *
    * @param dataFromBlob      Product DataFrame fetched from Azure Blob Storage
    * @param dataFromSQLServer Product DataFrame fetched from SQL Server
    * @return Product DataFrame with appropriate data types
    */
  def mergeAndTransformProductData(dataFromBlob: DataFrame, dataFromSQLServer: DataFrame): DataFrame = {
    dataFromBlob.union(dataFromSQLServer)
      .withColumn("order_detail_array", split(col("order_detail"), "~"))
      .withColumn("order_detail_exploded", explode(col("order_detail_array")))
      .withColumn("product", split(col("order_detail_exploded"), "\\|").getItem(0))
      .withColumn("aisles", split(col("order_detail_exploded"), "\\|").getItem(1))
      .withColumn("number_of_products", split(col("order_detail_exploded"), "\\|").getItem(2).cast(IntegerType))
      .drop("order_detail", "order_detail_array", "order_detail_exploded")
      .withColumn("product", regexp_replace(col("product"), "[^\\x00-\\x7F]", ""))
      .withColumn("days_since_prior_order", col("days_since_prior_order").cast(IntegerType))
      .withColumn("order_hour_of_day", when(col("order_hour_of_day") === 24, 0).
        otherwise(col("order_hour_of_day")))
  }

  /**
    * Validate that all columns in Product DataFrame have the correct range of values.
    * Notes:
    * Numeric columns cannot be < 0.
    * Delete white-spaces from string columns.
    */
  def validateDataValues(dfProduct: DataFrame): DataFrame = {
    dfProduct.schema.fields.foldLeft(dfProduct) {
      (df, column) => {
        column.dataType match {
          case _: StringType => df.withColumn(column.name, trim(df(column.name)))
          case _: IntegerType => df.withColumn(column.name, abs(df(column.name)))
          case _: LongType => df.withColumn(column.name, abs(df(column.name)))
          case _: FloatType => df.withColumn(column.name, abs(df(column.name)))
          case _: DoubleType => df.withColumn(column.name, abs(df(column.name)))
          case _ => df.withColumn(column.name, df(column.name))
        }
      }
    }
  }

  /**
    * A DataFrame it's returned with clients classified in different categories according to their orders behavior.
    *
    */
  def getClientsCategory(dfProducts: DataFrame): DataFrame = {
    val momItems = List("dairy eggs", "bakery", "household", "babies")
    val singleItems = List("canned goods", "meat seafood", "alcohol", "snacks", "beverages")
    val petFriendlyItems = List("canned goods", "pets", "frozen")

    // UDF to Classify clients according to the established rules
    val clientsCategoryUdf = udf((total: Int, mom: Int, single: Int, petFriendly: Int) => {
      var category: String = "A complete mystery"
      if (mom / total > 0.5) {
        category = "Mom"
      } else if (single / total > 0.6) {
        category = "Single"
      } else if (petFriendly / total > 0.3) {
        category = "Pet Friendly"
      }
      category
    })

    val window = Window.partitionBy("user_id")

    dfProducts.withColumn("total_products_bought", sum("number_of_products").over(window))
      .withColumn("mom_products", sum(when(col("department").isin(momItems: _*),
        col("number_of_products")).otherwise(0)).over(window))
      .withColumn("single_products", sum(when(col("department").isin(singleItems: _*),
        col("number_of_products")).otherwise(0)).over(window))
      .withColumn("pet_friendly_products", sum(when(col("department").isin(petFriendlyItems: _*),
        col("number_of_products")).otherwise(0)).over(window))
      .withColumn("category", clientsCategoryUdf(col("total_products_bought"), col("mom_products"),
        col("single_products"), col("pet_friendly_products")))
      .select("user_id", "category").dropDuplicates("user_id")
  }

  /**
    * A DataFrame it's returned with clients segmented according to the established rules.
    *
    */
  def getClientsSegmentation(dfProducts: DataFrame): DataFrame = {

    // UDF to Segment clients according to the established rules.
    def clientsSegmentUdf(m: Map[(String, Int), Double]) = udf((orderDow: Int, dspo: Int,
                                                                totalProductsBought: Int) => {
      var segment: String = "Undefined"
      if (dspo <= 7 && totalProductsBought > m("third", orderDow)) {
        segment = "You've Got a Friend in Me"
      } else if ((dspo >= 10 && dspo <= 19) && totalProductsBought > m("second", orderDow)) {
        segment = "Baby come Back"
      } else if (dspo > 20 && totalProductsBought > m("first", orderDow)) {
        segment = "Special Offers"
      }
      segment
    })

    // The Quartiles of every day are calculated and passed like param to the Client Segment UDF
    val quartileMap = Map.empty[(String, Int), Double]
    for (day <- (0 to 6).toList) {
      val medianAndQuantiles = dfProducts.filter(col("order_dow") === 1).stat.approxQuantile("number_of_products",
        Array(0.25,
        0.5, 0.75), 0.0)
      quartileMap(("first", day)) = medianAndQuantiles(0)
      quartileMap(("second", day)) = medianAndQuantiles(1)
      quartileMap(("third", day)) = medianAndQuantiles(2)
    }

    val window = Window.partitionBy("user_id")
    dfProducts.withColumn("total_products_bought", sum("number_of_products").over(window))
      .withColumn("client_segment", clientsSegmentUdf(quartileMap)(col("order_dow"),
        col("days_since_prior_order"), col("total_products_bought")))
      .select("user_id", "client_segment").dropDuplicates("user_id")
  }
}


