import org.apache.spark.sql.SparkSession

object StartETL {
  val usage =
    """
    Usage: ApplaudoETL.jar [-p string]
    Optional: [-p string]
    Description:
    -r: Result path.            Path to store parquet files.
    Notes:
      If the results path is not provided, the results will be printed in console
    """

  def main(args: Array[String]): Unit = {

    val argList = args.toList
    type OptionMap = Map[String, String]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "-r" :: value :: tail =>
          nextOption(map ++ Map("result_path" -> value), tail)
        case option :: tail => println("Unknown option " + option)
          throw new Exception(s"Unknown option\n\n$usage")
      }
    }

    val options = nextOption(Map(), argList)
    val spark = SparkSession.builder.appName("ApplaudoETL").master("local[*]").getOrCreate()

    val resultPath = if (options.contains("result_path")) options("result_path") else ""

    val applaudoETL = new ApplaudoETL(spark, resultPath)
    applaudoETL.start()
  }

}
