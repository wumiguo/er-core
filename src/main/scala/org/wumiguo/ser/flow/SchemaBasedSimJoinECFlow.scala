package org.wumiguo.ser.flow

import java.util.Calendar

import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.wumiguo.ser.dataloader.JSONWrapper
import org.wumiguo.ser.entity.parameter.DatasetConfig
import org.wumiguo.ser.methods.datastructure.WeightedEdge
import org.wumiguo.ser.methods.entityclustering.ConnectedComponentsClustering
import org.wumiguo.ser.methods.similarityjoins.common.CommonFunctions
import org.wumiguo.ser.methods.similarityjoins.simjoin.{EDJoin, PartEnum}

import scala.collection.mutable.ArrayBuffer

/**
  * @author johnli
  *         Created on 2020/6/18
  *         (Change file header on Settings -> Editor -> File and Code Templates)
  */
object SchemaBasedSimJoinECFlow extends ERFlow {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SchemaBasedSimJoinECFlow")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "4")
      .set("spark.local.dir", "/data2/tmp")

    var context = new SparkContext(conf)
    val dataset1Path = getParameter(args, "dataset1", "datasets\\clean\\DblpAcm\\dataset1.json")
    val dataset1Format = getParameter(args, "dataset1-format", "json")
    val dataset1Id = getParameter(args, "dataset1-id", "realProfileID")
    val attributes1 = getParameter(args, "attributes1", "title")
    val dataset2Path = getParameter(args, "dataset2", "datasets\\clean\\DblpAcm\\dataset2.json")
    val dataset2Format = getParameter(args, "dataset2-format", "json")
    val dataset2Id = getParameter(args, "dataset2-id", "realProfileID")
    val attributes2 = getParameter(args, "attributes2", "title")
    val q = getParameter(args, "q", "2")
    val threshold = getParameter(args, "threshold", "2")

    val algorithm = getParameter(args, "algorithm", "PartEnum")

    val dataset1 = new DatasetConfig(dataset1Path, dataset1Format, dataset1Id,
      Option(attributes1).map(_.split(",")).orNull)
    val dataset2 = new DatasetConfig(dataset2Path, dataset2Format, dataset2Id,
      Option(attributes2).map(_.split(",")).orNull)

    val logPath = "ss-join.log"

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout()
    val appender = new FileAppender(layout, logPath, false)
    log.addAppender(appender)

    val profiles1 = JSONWrapper.loadProfiles(dataset1.path, realIDField = dataset1.dataSetId)
    val profiles2 = JSONWrapper.loadProfiles(dataset2.path, realIDField = dataset2.dataSetId, startIDFrom = profiles1.count().intValue())

    assert(dataset2.path == null || dataset1.attribute.length == dataset2.attribute.length,
      "If dataset 2 exist, the number of attribute use to compare between dataset 1 and dataset 2 should be equal")

    var attributesArray = new ArrayBuffer[(RDD[(Int, String)], RDD[(Int, String)])]()

    for (i <- 0 until dataset1.attribute.length) {
      val attributes1 = CommonFunctions.extractField(profiles1, dataset1.attribute(i))
      val attributes2 = Option(dataset2.attribute).map(attributes => CommonFunctions.extractField(profiles2, attributes(i))).orNull
      attributesArray += ((attributes1, attributes2))
    }

    val t1 = Calendar.getInstance().getTimeInMillis
    var attributesMatches = new ArrayBuffer[RDD[(Int, Int, Double)]]()
    var attributeses = ArrayBuffer[RDD[(Int, String)]]()
    attributesArray.foreach(attributesTuple => {
      val attributes1 = attributesTuple._1
      val attributes2 = attributesTuple._2
      val attributesMatch: RDD[(Int, Int, Double)] =
        algorithm match {
          case "EDJoin" =>
            val attributes = attributes1.union(attributes2)
            attributeses += attributes
            attributes.cache()
            EDJoin.getMatches(attributes, q.toInt, threshold.toInt)
          case "PartEnum" =>
            attributes1.cache()
            attributes2.cache()
            PartEnum.getMatches(attributes1, attributes2, 0.9)
        }
      attributesMatches += attributesMatch
    })

    val t2 = Calendar.getInstance().getTimeInMillis

    log.info("[EDJoin] Global join+verification time (s) " + (t2 - t1) / 1000.0)

    var matches = attributesMatches(0);

    for (i <- 1 until attributesMatches.length) {
      matches = matches.intersection(attributesMatches(i))
    }

    if (attributesMatches.length > 1) matches.cache()

    val nm = matches.count()
    val t3 = Calendar.getInstance().getTimeInMillis
    attributesArray.foreach(attributesTuple => {
      Option(attributesTuple._1).map(_.unpersist())
      Option(attributesTuple._2).map(_.unpersist())
    })
    attributeses.foreach(_.unpersist())
    log.info("[EDJoin] Number of matches " + nm)
    log.info("[EDJoin] Intersection time (s) " + (t3 - t2) / 1000.0)

    val profiles = profiles1.union(profiles2)
    val clusters = ConnectedComponentsClustering.getClusters(profiles, matches.map(x => WeightedEdge(x._1, x._2, 0)), 0)
    clusters.cache()
    val cn = clusters.count()
    val t4 = Calendar.getInstance().getTimeInMillis
    log.info("[EDJoin] Number of clusters " + cn)
    log.info("[EDJoin] Clustering time (s) " + (t4 - t3) / 1000.0)

    log.info("[EDJoin] Total time (s) " + (t4 - t1) / 1000.0)
  }

  def getParameter(args: Array[String], name: String, defaultValue: String = null): String = {
    var parameterPair = args.find(_.startsWith(name + "=")).orNull
    if (null != parameterPair)
      parameterPair.split("=")(1)
    else
      defaultValue
  }
}
