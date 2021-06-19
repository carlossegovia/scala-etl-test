import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.Map

object applaudo_etl {

  val key: String = "?sv=2019-12-12&ss=bfqt&srt=sco&sp=rlx&se=2030-07-28T18:45:41Z&st=2020-07-27T10:45:41Z&spr=https" +
    "&sig" +
    "=cJncLH0UHtfEK1txVC2BNCAwJqvcBrAt5QS2XeL9bUE%3D"
  val containerName: String = "ordersdow"
  val storageAccountName: String = "orderstg"
  val productSchema = Encoders.product[Product].schema
  // BigQuery parameters
  val bucket = "test-bucket-concrete-flare-312721"


  def main(args: Array[String]): Unit = {

    //    val spark = SparkSession.builder.appName("ScalaTest").master("local[*]").getOrCreate()
    val spark = SparkSession.builder.appName("AppaludoETL").getOrCreate()
    spark.conf.set(s"fs.azure.sas.$containerName.$storageAccountName.blob.core.windows.net", key)
    spark.conf.set("temporaryGcsBucket", bucket)

    // Merge both Product's DataFrames
    val dfProducts = mergeProductData(getDataFromBlobStorage(spark), getDataFromSQLServer(spark))

    // Test DataFrame from API
    val dfProductDetails = getDataFromAPI(spark).withColumnRenamed("aisle", "aisle_pd")

    // Join datasets
    val dfJoined = dfProducts.join(broadcast(dfProductDetails), dfProducts("product") === dfProductDetails
    ("product_name"), "left").drop("aisle_pd", "product_name")

    // Clean and validate data, then write into a BigQuery table
    validateData(dfJoined).write.mode("overwrite").format("bigquery")
      .option("table", "test.products")
      .save()

    createClientsCategory(validateData(dfJoined))
    createClientsSegmentation(validateData(dfJoined))

  }

  def getDataFromBlobStorage(spark: SparkSession): DataFrame = {
    val path = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net"
    spark.read.schema(productSchema)
      .option("header", "false")
      .option("escape", "\"")
      .option("mode", "DROPMALFORMED").csv(path + "/0*.csv")

  }


  def getDataFromSQLServer(spark: SparkSession): DataFrame = {
    val df = spark.read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://orderservers.database.windows.net;database=orderdb")
      .option("dbtable", "dbo.order_details")
      .option("user", "etlguest")
      .option("password", "Etltest_2020")
      .load()

    productSchema.fields.foldLeft(df) {
      (df, s) => df.withColumn(s.name, df(s.name).cast(s.dataType))
    }
  }

  def getDataFromAPI(spark: SparkSession): DataFrame = {
    val url = "https://etlwebapp.azurewebsites.net/api/products"
    implicit val formats = DefaultFormats
    val data = List(scala.io.Source.fromURL(url).mkString)(0)
    val jsonData = parse(data)
    val elements = (jsonData \\ "results" \\ "items").children.map(_.extract[ProductDetail])
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    elements.toDF()
  }

  def mergeProductData(dataFromBlob: DataFrame, dataFromSQLServer: DataFrame): DataFrame = {
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

  def validateData(df: DataFrame): DataFrame = {
    df.schema.fields.foldLeft(df) {
      (df, s) => {
        s.dataType match {
          case _: StringType => df.withColumn(s.name, trim(df(s.name)))
          case _: IntegerType => df.withColumn(s.name, abs(df(s.name)))
          case _: LongType => df.withColumn(s.name, abs(df(s.name)))
          case _: FloatType => df.withColumn(s.name, abs(df(s.name)))
          case _: DoubleType => df.withColumn(s.name, abs(df(s.name)))
          case _ => df.withColumn(s.name, df(s.name))
        }
      }
    }
  }

  def createClientsCategory(df: DataFrame): Unit = {
    val momItems = List("dairy eggs", "bakery", "household", "babies")
    val singleItems = List("canned goods", "meat seafood", "alcohol", "snacks", "beverages")
    val petFriendlyItems = List("canned goods", "pets", "frozen")

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
    val newDf = df.withColumn("total_products_bought", sum("number_of_products").over(window))
      .withColumn("mom_products", sum(when(col("department").isin(momItems: _*),
        col("number_of_products")).otherwise(0)).over(window))
      .withColumn("single_products", sum(when(col("department").isin(singleItems: _*),
        col("number_of_products")).otherwise(0)).over(window))
      .withColumn("pet_friendly_products", sum(when(col("department").isin(petFriendlyItems: _*),
        col("number_of_products")).otherwise(0)).over(window))
      .withColumn("category", clientsCategoryUdf(col("total_products_bought"), col("mom_products"),
        col("single_products"), col("pet_friendly_products")))
      .select("user_id", "category").dropDuplicates("user_id")

    newDf.write.mode("overwrite").format("bigquery")
      .option("table", "test.clients_category")
      .save()
  }

  def createClientsSegmentation(df: DataFrame): Unit = {

    def clientsSegmentUdf(m: Map[(String, Int), Double]) = udf((orderDow: Int, dspo: Int,
                                                                totalProductsBought: Int) => {

      var segment: String = "Without segment"
      if (dspo <= 7 && totalProductsBought > m("third", orderDow)) {
        segment = "You've Got a Friend in Me"
      } else if ((dspo >= 10 && dspo <= 19) && totalProductsBought > m("second", orderDow)) {
        segment = "Baby come Back"
      } else if (dspo > 20 && totalProductsBought > m("first", orderDow)) {
        segment = "Special Offers"
      }
      segment
    })

    val quartileMap = Map.empty[(String, Int), Double]

    for (row <- df.groupBy("order_dow", "order_hour_of_day").avg("number_of_products").collect) {
      val day = row.mkString(",").split(",")(0).toInt
      val hour = row.mkString(",").split(",")(1).toInt
      val value = row.mkString(",").split(",")(2).toDouble

      hour match {
        case h if 0 to 5 contains h =>
          if (quartileMap.contains(("first", day))) quartileMap(("first", day)) += value else quartileMap(("first",
            day)) = value
        case h if 6 to 11 contains h =>
          if (quartileMap.contains(("second", day))) quartileMap(("second", day)) += value else quartileMap(("second",
            day)) = value
        case h if 12 to 17 contains h =>
          if (quartileMap.contains(("third", day))) quartileMap(("third", day)) += value else quartileMap(("third",
            day)) = value
        case h if 18 to 23 contains h =>
          if (quartileMap.contains(("fourth", day))) quartileMap(("fourth", day)) += value else quartileMap(("fourth",
            day)) = value
      }
    }

    val window = Window.partitionBy("user_id")
    val newDf = df.withColumn("total_products_bought", sum("number_of_products").over(window))
      .withColumn("client_segment", clientsSegmentUdf(quartileMap)(col("order_dow"),
        col("days_since_prior_order"), col("total_products_bought")))
      .select("user_id", "client_segment").dropDuplicates("user_id")

    newDf.write.mode("overwrite").format("bigquery")
      .option("table", "test.clients_segmentation")
      .save()
  }
}


case class ProductDetail(product_name: String, aisle: String, department: String)

case class Product(order_id: Long, user_id: Long, order_number: Int, order_dow: Int, order_hour_of_day: Int,
                   days_since_prior_order: Float, order_detail: String)