import java.io.InputStream
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable

case class ProductDetail(product_name: String, aisle: String, department: String)

case class Product(order_id: Long, user_id: Long, order_number: Int, order_dow: Int, order_hour_of_day: Int,
                   days_since_prior_order: Float, order_detail: String)


class ApplaudoETL(spark: SparkSession, resultPath: String, productsTableName: String = "products",
                  clientsTableName: String = "clients") {

  private val properties = getProperties
  private val SasKey = properties.getProperty("azure.sas_key")
  private val ContainerName = properties.getProperty("azure.storage.container.name")
  private val StorageAccountName = properties.getProperty("azure.storage.account.name")
  private val ProductSchema = Encoders.product[Product].schema


  def start(): Unit = {

    spark.conf.set(s"fs.azure.sas.$ContainerName.$StorageAccountName.blob.core.windows.net", SasKey)

    // Merge Product's data from Azure Blob Storage and SQL Server
    val dfProducts = mergeAndTransformProductData(getDataFromBlobStorage(spark), getDataFromSQLServer(spark))

    // Get Product's Detail data from API
    val dfProductDetails = getDataFromAPI(spark).withColumnRenamed("aisle", "aisle_pd")

    // Join Product's and Product's Detail data
    // `broadcast` is used to get better performance and because the DataFrame of ProductDetails is small enough
    val dfProductJoined = dfProducts.join(broadcast(dfProductDetails), dfProducts("product") === dfProductDetails
    ("product_name"), "left").drop("aisle_pd", "product_name")

    // Clean and validate data values
    val dfValidated = validateDataValues(dfProductJoined)

    // Show on Console or write parquet files
    if (!resultPath.isEmpty) {
      storeData(dfValidated, productsTableName)
    } else {
      dfValidated.show(10, truncate = false)
    }

    // Create the table with the Clients Category and Segmentation
    val dfCategory = getClientsCategory(dfValidated)
    val dfSegmentation = getClientsSegmentation(dfValidated)
    val dfClients = dfCategory.join(dfSegmentation, Seq("user_id"))

    // Show on Console or write parquet files
    if (!resultPath.isEmpty) {
      storeData(dfClients, clientsTableName)
    } else {
      dfClients.show(10, truncate = false)
    }
  }

  /**
    * Fetch Products data from Azure Blob Storage and return it in a DataFrame.
    * A list of files are fetched according to the `fileNumber` parameter, by default fileNumber = -1, this means
    * that all data in the directory will be returned.
    * By providing a value for `fileNumber` parameter, new files can be consumed from the Directory without repeating
    * those already obtained in previous executions. This works by assuming that the new data will arrive in files
    * with higher values names.
    */
  def getDataFromBlobStorage(spark: SparkSession, fileNumber: Int = -1): DataFrame = {
    val path = s"wasbs://$ContainerName@$StorageAccountName.blob.core.windows.net"
    spark.read.csv(s"$path/00.csv")
    val listFiles = getFileNames(path, fileNumber)

    spark.read.schema(ProductSchema)
      .option("header", "false")
      .option("escape", "\"")
      .option("mode", "DROPMALFORMED").csv(listFiles: _*)
  }

  /**
    * Return the list of files inside a Directory that have a higher numeric value of the `fileNumber` parameter.
    * e.g. `fileNumber` = 2 and there exists these files in a directory: 01.csv, 02.csv, 03.csv and 04.csv
    * The output will be an Array with 03.csv and 04.csv
    *
    */
  def getFileNames(path: String, fileNumber: Int): Array[String] = {
    val fs = FileSystem.get(new java.net.URI(path), new Configuration())
    val status = fs.listStatus(new Path(path))
    status.filter(x => x.getPath.toString.split("/").last.split("\\.").head.toInt > fileNumber)
      .map(x => x.getPath.toString)
  }

  /**
    * Fetch Products data from a SQL Server instance and return it in a DataFrame.
    * The data is fetched using a custom query that compares the field `order_id`, by default orderId = -1, this means
    * that all data in the table will be returned.
    * By providing a value for `order_id` field, new records can be consumed from the SQL Server table without
    * repeating those already obtained in previous executions, since each `order_id` field is unique and it is assumed
    * that the new data will have higher values.
    *
    */
  def getDataFromSQLServer(spark: SparkSession, orderId: Int = -1): DataFrame = {

    val query = s"(select * from ${properties.getProperty("mssql.dbtable")} where CAST(order_id AS int) > $orderId) " +
      s"as t"

    val df = spark.read
      .format("jdbc")
      .option("driver", properties.getProperty("mssql.driver"))
      .option("url", properties.getProperty("mssql.url"))
      .option("dbtable", query)
      .option("user", properties.getProperty("mssql.user"))
      .option("password", properties.getProperty("mssql.password"))
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
    val url = properties.getProperty("api.url")
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
    def clientsSegmentUdf(m: mutable.Map[(String, Int), Double]) = udf((orderDow: Int, dspo: Int,
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
    // The function approxQuantile is used with 0.0 in the third argument, this way the results are exact and not
    // approximations
    val quartileMap = mutable.Map.empty[(String, Int), Double]
    for (day <- (0 to 6).toList) {
      val medianAndQuantiles = dfProducts.filter(col("order_dow") === day).stat.approxQuantile("number_of_products",
        Array(0.25, 0.5, 0.75), 0.0)
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

  /**
    * Get necessary properties to access Azure Blob Storage, MSSQL and API data
    *
    */
  def getProperties: Properties = {
    val stream: InputStream = getClass.getResourceAsStream("application.properties")
    val properties = new Properties()
    properties.load(stream)
    properties
  }

  /**
    * Write parquet files with the ETL results.
    * The `tableName` parameter allows storing new data from different executions in different places
    * This method could be easily changed to store the data in many DB's like BigQuery, Hive, Redshift, etc.
    *
    */
  def storeData(df: DataFrame, tableName: String): Unit = {
    df.write.mode("overwrite").parquet(s"$resultPath/$tableName")
  }
}


