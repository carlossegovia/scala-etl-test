import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object applaudo_etl {

  val key: String = "?sv=2019-12-12&ss=bfqt&srt=sco&sp=rlx&se=2030-07-28T18:45:41Z&st=2020-07-27T10:45:41Z&spr=https" +
    "&sig" +
    "=cJncLH0UHtfEK1txVC2BNCAwJqvcBrAt5QS2XeL9bUE%3D"
  val containerName: String = "ordersdow"
  val storageAccountName: String = "orderstg"
  val productSchema = Encoders.product[Product].schema
  val bucket = "test-bucket-concrete-flare-312721"
  // BigQuery parameters


  def main(args: Array[String]): Unit = {

    //    val spark = SparkSession.builder.appName("ScalaTest").master("local[*]").getOrCreate()
    val spark = SparkSession.builder.appName("AppaludoETL").getOrCreate()
    spark.conf.set(s"fs.azure.sas.$containerName.$storageAccountName.blob.core.windows.net", key)
    spark.conf.set("temporaryGcsBucket", bucket)

    // Test DataFrame from API
    val dfProductDetails = getDataFromAPI(spark).withColumnRenamed("aisle", "aisle_json")

    // Transform both DataFrames
    val dfProducts = transformData(getDataFromBlobStorage(spark), getDataFromSQLServer(spark))

    // Join datasets
    val dfJoined = dfProducts.join(broadcast(dfProductDetails), dfProducts("product") === dfProductDetails
    ("product_name"), "left")

    dfProductDetails.write.format("bigquery")
      .option("table", "test.product_details")
      .save()

    // Validate Data
    validateData(dfJoined).write.format("bigquery")
      .option("table", "test.products")
      .save()

  }

  def getDataFromBlobStorage(spark: SparkSession): DataFrame = {
    val path = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net"
    spark.read.schema(productSchema).option("header", "false").option("mode", "DROPMALFORMED").csv(path + "/0*.csv")

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

  def transformData(dataFromBlob: DataFrame, dataFromSQLServer: DataFrame): DataFrame = {
    dataFromBlob.union(dataFromSQLServer)
      .withColumn("order_detail_array", split(col("order_detail"), "~"))
      .withColumn("order_detail_exploded", explode(col("order_detail_array")))
      .withColumn("product", split(col("order_detail_exploded"), "\\|").getItem(0))
      .withColumn("aisles", split(col("order_detail_exploded"), "\\|").getItem(1))
      .withColumn("number_of_products", split(col("order_detail_exploded"), "\\|").getItem(2).cast(IntegerType))
      .drop("order_detail", "order_detail_array", "order_detail_exploded")
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
}


case class ProductDetail(product_name: String, aisle: String, department: String)

case class Product(order_id: Long, user_id: Long, order_number: Int, order_dow: Int, order_hour_of_day: Int,
                   days_since_prior_order: Float, order_detail: String)