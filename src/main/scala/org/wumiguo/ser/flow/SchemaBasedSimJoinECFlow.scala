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
import org.wumiguo.ser.methods.similarityjoins.simjoin.EDJoin

import scala.collection.mutable.ArrayBuffer

/**
  * @author johnli
  *         Created on 2020/6/18
  *         (Change file header on Settings -> Editor -> File and Code Templates)
  */
object SchemaBasedSimJoinECFlow extends ERFlow {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "4")
      .set("spark.local.dir", "/data2/tmp")

    var context = new SparkContext(conf)
    val dataset1Path = getParameter(args, "dataset1", "C:\\Users\\rinanzhi\\IdeaProjects\\er-spark\\datasets\\clean\\DblpAcm\\dataset1.json")
    val dataset1Format = getParameter(args, "dataset1-format", "json")
    val dataset2Path = getParameter(args, "dataset2", "C:\\Users\\rinanzhi\\IdeaProjects\\er-spark\\datasets\\clean\\DblpAcm\\dataset2.json")
    val dataset2Format = getParameter(args, "dataset2-format", "json")
    val attributeNames = getParameter(args, "attributes", "year,title")
    val dataset1 = new DatasetConfig(dataset1Path, dataset1Format)
    val dataset2 = new DatasetConfig(dataset2Path, dataset2Format)

    val logPath = "C:\\Users\\rinanzhi\\IdeaProjects\\er-spark\\ACMlog.txt"

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout()
    val appender = new FileAppender(layout, logPath, false)
    log.addAppender(appender)

    val profiles = JSONWrapper.loadProfiles(dataset1.path, realIDField = "realProfileID").
      union(JSONWrapper.loadProfiles(dataset2.path, realIDField = "realProfileID"))

    var attributesArray = new ArrayBuffer[RDD[(Int, String)]]()

    attributeNames.split(",").foreach(attributeName => {
      var attributes = CommonFunctions.extractField(profiles, attributeName)
      attributes.cache()
      attributes.count()
      attributesArray += attributes
    })

    val t1 = Calendar.getInstance().getTimeInMillis
    var attributesMatches = new ArrayBuffer[RDD[(Int, Int)]]()
    attributesArray.foreach(attributes => {
      val attributesMatch = EDJoin.getMatches(attributes, 2, 2)
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
    attributesArray.foreach(attributes => attributes.unpersist())
    log.info("[EDJoin] Number of matches " + nm)
    log.info("[EDJoin] Intersection time (s) " + (t3 - t2) / 1000.0)

    val clusters = ConnectedComponentsClustering.getClusters(profiles, matches.map(x => WeightedEdge(x._1, x._2, 0)), 0)
    clusters.cache()
    val cn = clusters.count()
    val t4 = Calendar.getInstance().getTimeInMillis
    log.info("[EDJoin] Number of clusters " + cn)
    log.info("[EDJoin] Clustering time (s) " + (t4 - t3) / 1000.0)

    log.info("[EDJoin] Total time (s) " + (t4 - t1) / 1000.0)

    clusters.foreach(println(_))
  }

  def getParameter(args: Array[String], name: String, defaultValue: String = null): String = {
    var parameterPair = args.find(_.startsWith(name + "=")).orNull
    if (null != parameterPair)
      parameterPair.split("=")(1)
    else
      defaultValue
  }
}
