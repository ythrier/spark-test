import data.{Councillor, CouncillorDataReader}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkPlayground {
  val PDF_FILE: String = "src/main/resources/interessen.pdf"

  def main(args: Array[String]) {
    val councillors = CouncillorDataReader.extractFrom(PDF_FILE, 7, 57)
    val conf = new SparkConf().setMaster("local").setAppName("Sparky")
    val sc = new SparkContext(conf)
    val councillorsRDD = sc.parallelize(councillors)
    // ... something to test, do your own stuff :-)
    val numberOfMandates = councillorsRDD.flatMap(x => x.mandates).count()
    println(numberOfMandates)
  }
}
