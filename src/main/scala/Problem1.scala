import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object Problem1 extends App {
  if (args.length != 2) {
    println("Usage: [Input file path] [output file path]")
    sys.exit(1)
  }

  //  val conf = new SparkConf().setMaster("local").setAppName("My App")
  //  val sc = new SparkContext(conf)

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Problem1")
    //    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  val sc = spark.sparkContext

  val a = sc.textFile(args(0))

  val schemaString = "transID custID transTotal transNumItems transDesc"

  val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))

  val schema = StructType(fields)

  val rowRDD = a.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4).trim))

  val transDataFrame = spark.createDataFrame(rowRDD, schema)

  transDataFrame.createOrReplaceTempView("transectionView")

  val T1 = spark.sql("SELECT * FROM transectionView WHERE transTotal>=200")
  transDataFrame.registerTempTable("T1")
  T1.show()

  var T2 = spark.sql("SELECT transNumItems, sum(transTotal) as sum, avg(transTotal)as avg, min(transTotal)as min, max(transTotal) as max FROM T1 GROUP BY transNumItems")
  transDataFrame.registerTempTable("T2")
  T2.show()
  T2.rdd.repartition(1).saveAsTextFile(args(1) + "/T2")

  var T3 = spark.sql("SELECT custID, count(*) as count_transection FROM T1 GROUP BY custID")
  T3.registerTempTable("T3")
  T3.show()

  val T4 = spark.sql("SELECT * FROM transectionView WHERE transTotal>600")
  transDataFrame.registerTempTable("T4")
  T4.show()

  var T5 = spark.sql("SELECT custID, count(*) as count_transection FROM T4 GROUP BY custID")
  T5.registerTempTable("T5")
  T5.show()

  var T6 = spark.sql("SELECT T5.custID FROM T5 JOIN T3 ON T5.custID=T3.custID WHERE T5.count_transection * 3 > T3.count_transection GROUP BY T5.custID")
  T6.show()
  T6.rdd.repartition(1).saveAsTextFile(args(1) + "/T6")
}