import java.io.{BufferedOutputStream, File, FileOutputStream}

import org.apache.spark.sql.{DataFrame, SQLContext}
import sqlContext.implicits._

import data.{Committee, Councillor, CouncillorDataReader, InputFile}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkPlayground {
  val NR_INTERESTS = "src/main/resources/ra-nr-interessen.pdf"
  val SR_INTERESTS = "src/main/resources/ra-sr-interessen.pdf"

  def main(args: Array[String]) {
    val dataFiles = List(new InputFile(NR_INTERESTS, 7, 57), new InputFile(SR_INTERESTS, 7, 22))
    val councillors = CouncillorDataReader.extractFromFiles(dataFiles)
    exportCouncillors(councillors)
    val conf = new SparkConf().setMaster("local").setAppName("Sparky")
    val sc = new SparkContext(conf)
    //    val councillorsRDD = sc.parallelize(councillors)
    //    doSpark(councillorsRDD)
  }

  private def exportCouncillors(councillors: List[Councillor]): Unit = {
    val bos = new BufferedOutputStream(new FileOutputStream(new File("target/data.txt")))
    var id = 0
    councillors.foreach(x => {
      val councillorString = id + ";" + x.name + ";" + x.profession + ";" + x.party
      x.mandates.foreach(y => {
        val mandateString = id + ";" + y.name + ";" + y.legalForm + ";" + y.committee + ";" + y.position
        val data = councillorString + ";" + mandateString + "\n"
        bos.write(data.getBytes("UTF-8"))
      })
      id += 1
    })
    bos.flush()
    bos.close()
  }

  private def doSpark(councillorsRDD: RDD[Councillor]) = {
    orderByNumberOfMandates(councillorsRDD)
    //    groupByMandates(councillorsRDD)
    //    topOfGroupByMandates(councillorsRDD)
    //    numberOfVRMandatesComparedToNumberOfMandates(councillorsRDD)
    //    groupByMandateCommittees(councillorsRDD)
  }

  private def groupByMandateCommittees(councillorsRDD: RDD[Councillor]) = {
    println("===Mandate grouped by committees (sorted count)==============")
    val groupByMandateCommittees = councillorsRDD
      .flatMap(c => c.mandates)
      .groupBy(m => m.committee)
      .sortBy(g => (-1) * g._2.size)
      .collect()
    val numberOfMandates = councillorsRDD
      .flatMap(c => c.mandates)
      .count()
    groupByMandateCommittees.foreach(x => {
      println(x._1, " " + x._2.size, " " + (100.0 / numberOfMandates.toDouble * x._2.size.toDouble).toInt + "%")
    })
    println("=============================================================")
  }

  private def numberOfVRMandatesComparedToNumberOfMandates(councillorsRDD: RDD[Councillor]) = {
    println("===Number of VR mandates vs. number of mandates==============")
    val numberOfVRMandates = councillorsRDD
      .flatMap(c => c.mandates)
      .filter(m => m.committee.equals(Committee.administrationBoard))
      .count()
    val numberOfMandates = councillorsRDD
      .flatMap(c => c.mandates)
      .count()
    println("Number of VR mandates: " + numberOfVRMandates +
      "(" + (100.0 / numberOfMandates.toDouble * numberOfVRMandates.toDouble).toInt + "%)",
      " Number of mandates: " + numberOfMandates)
    println("=============================================================")
  }

  private def topOfGroupByMandates(councillorsRDD: RDD[Councillor]) = {
    val topX = 10
    println("===Top " + topX + " of mandates with involved councillors===")
    val topOfGroupByMandates = councillorsRDD
      .flatMap(c => c.mandates.map(m => (c, m)))
      .groupBy(cm => cm._2.name)
      .map(g => g._2)
      .sortBy(cm => (-1) * cm.size)
      .take(topX)
    topOfGroupByMandates.foreach(x => {
      println(x.head._2.name, x.size)
      x.foreach(y => println(y._1.name, y._2.position))
      println("--------------------------------------")
    })
    println("=============================================================")
  }

  private def groupByMandates(councillorsRDD: RDD[Councillor]) = {
    println("===Mandates and number of involved councillors===============")
    val groupByMandates = councillorsRDD
      .flatMap(councillor => councillor.mandates.map(mandate => (mandate.name, councillor.name)))
      .groupBy(councillorMandateTuple => councillorMandateTuple._1)
      .sortBy(councillorMandateTuple => (-1) * councillorMandateTuple._2.size).aggregate(0)()
      .collect()
    groupByMandates.foreach(x => println(x._1, x._2.size))
    println("=============================================================")
  }

  private def orderByNumberOfMandates(councillorsRDD: RDD[Councillor]) = {
    println("===Councillors with number of mandates=======================")
    orderByNumberOfMandatesSpark(councillorsRDD)
    orderByNumberOfMandatesSql(councillorsRDD)
    println("=============================================================")
  }

  private def orderByNumberOfMandatesSql(councillorsRDD: RDD[Councillor]) = {
    val sparkContext = councillorsRDD.sparkContext
    val sc = new SQLContext(sparkContext)
    val df = sc.createDataFrame(councillorsRDD)

  }

  private def orderByNumberOfMandatesSpark(councillorsRDD: RDD[Councillor]): Unit = {
    val orderByNumberOfMandates = councillorsRDD
      .sortBy(x => (-1) * x.mandates.size)
      .collect()
    orderByNumberOfMandates.foreach(x => println(x.name, x.mandates.size))
  }
}