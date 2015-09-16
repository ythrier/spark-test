import data.{Councillor, CouncillorDataReader}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkPlayground {
  val NR_INTERESTS = "src/main/resources/ra-nr-interessen.pdf"
  val SR_INTERESTS = "src/main/resources/ra-nr-interessen.pdf"

  def main(args: Array[String]) {
    val nrCouncillors = CouncillorDataReader.extractFrom(NR_INTERESTS, 7, 57)
    val srCouncillors = CouncillorDataReader.extractFrom(SR_INTERESTS, 7, 22)
    val councillors = nrCouncillors ::: srCouncillors
    val conf = new SparkConf().setMaster("local").setAppName("Sparky")
    val sc = new SparkContext(conf)
    val councillorsRDD = sc.parallelize(councillors)
    doSpark(councillorsRDD)
  }

  private def doSpark(councillorsRDD: RDD[Councillor]) = {
    orderByNumberOfMandates(councillorsRDD)
    groupByMandates(councillorsRDD)
    topOfGroupByMandates(councillorsRDD)
  }

  def topOfGroupByMandates(councillorsRDD: RDD[Councillor]) = {
    val topX = 10
    println("===Top " + topX + " of mandates with involvedc councillors===")
    val topOfGroupByMandates = councillorsRDD
      .flatMap(councillor => councillor.mandates.map(mandate => (mandate.name, (councillor.name, councillor.party))))
      .groupBy(councillorMandateTuple => councillorMandateTuple._1)
      .sortBy(councillorMandateTuple => (-1) * councillorMandateTuple._2.size)
      .take(topX)
    topOfGroupByMandates.foreach(x => {
      println(x._1, x._2.size)
      x._2.foreach(y => println(y._2))
      println()
    })
    println("=============================================================")
  }

  private def groupByMandates(councillorsRDD: RDD[Councillor]) = {
    println("===Mandates and number of involved councillors===============")
    val groupByMandates = councillorsRDD
      .flatMap(councillor => councillor.mandates.map(mandate => (mandate.name, councillor.name)))
      .groupBy(councillorMandateTuple => councillorMandateTuple._1)
      .sortBy(councillorMandateTuple => (-1) * councillorMandateTuple._2.size)
      .collect()
    groupByMandates.foreach(x => println(x._1, x._2.size))
    println("=============================================================")
  }

  private def orderByNumberOfMandates(councillorsRDD: RDD[Councillor]) = {
    println("===Councillors with number of mandates=======================")
    val orderByNumberOfMandates = councillorsRDD
      .sortBy(x => (-1) * x.mandates.size)
      .collect()
    orderByNumberOfMandates.foreach(x => println(x.name, x.mandates.size))
    println("=============================================================")
  }
}