package org.wumiguo.ser.flow

import java.util.Calendar

import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.wumiguo.ser.dataloader.{JSONWrapper, ProfileLoaderFactory, ProfileLoaderTrait}
import org.wumiguo.ser.entity.parameter.DatasetConfig
import org.wumiguo.ser.methods.datastructure.{Profile, WeightedEdge}
import org.wumiguo.ser.methods.entityclustering.ConnectedComponentsClustering
import org.wumiguo.ser.methods.similarityjoins.common.CommonFunctions
import org.wumiguo.ser.methods.similarityjoins.simjoin.{EDJoin, PartEnum}
import org.wumiguo.ser.methods.util.CommandLineUtil

import scala.collection.mutable.ArrayBuffer

/**
  * @author johnli
  *         Created on 2020/6/18
  *         (Change file header on Settings -> Editor -> File and Code Templates)
  */
object SchemaBasedSimJoinECFlow extends ERFlow {

  private val ALGORITHM_EDJOIN = "EDJoin"
  private val ALGORITHM_PARTENUM = "PartEnum"

  override def run(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SchemaBasedSimJoinECFlow")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "4")
      .set("spark.local.dir", "/data2/tmp")

    var context = new SparkContext(conf)
    val dataset1Path = CommandLineUtil.getParameter(args, "dataset1", "datasets\\clean\\DblpAcm\\dataset1.json")
    val dataset1Format = CommandLineUtil.getParameter(args, "dataset1-format", "json")
    val dataset1Id = CommandLineUtil.getParameter(args, "dataset1-id", "realProfileID")
    val attributes1 = CommandLineUtil.getParameter(args, "attributes1", "title")
    val dataset2Path = CommandLineUtil.getParameter(args, "dataset2", "datasets\\clean\\DblpAcm\\dataset2.json")
    val dataset2Format = CommandLineUtil.getParameter(args, "dataset2-format", "json")
    val dataset2Id = CommandLineUtil.getParameter(args, "dataset2-id", "realProfileID")
    val attributes2 = CommandLineUtil.getParameter(args, "attributes2", "title")
    val q = CommandLineUtil.getParameter(args, "q", "2")
    val threshold = CommandLineUtil.getParameter(args, "threshold", "2")

    val algorithm = CommandLineUtil.getParameter(args, "algorithm", ALGORITHM_EDJOIN)

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

    def profileLoader: ProfileLoaderTrait = getProfileLoader(dataset1.path)

    val profiles1 = profileLoader.load(dataset1.path, realIDField = dataset1.dataSetId, startIDFrom = 0, sourceId = 0)
    val profiles2 = profileLoader.load(dataset2.path, realIDField = dataset2.dataSetId, startIDFrom = profiles1.count().intValue(), sourceId = 1)

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
          case ALGORITHM_EDJOIN =>
            val attributes = attributes1.union(attributes2)
            attributeses += attributes
            attributes.cache()
            EDJoin.getMatches(attributes, q.toInt, threshold.toInt)
          case ALGORITHM_PARTENUM =>
            val attributes = attributes1.union(attributes2)
            attributeses += attributes
            attributes.cache()
            PartEnum.getMatches(attributes, threshold.toDouble)
        }
      attributesMatches += attributesMatch
    })

    val t2 = Calendar.getInstance().getTimeInMillis

    log.info("[SSJoin] Global join+verification time (s) " + (t2 - t1) / 1000.0)

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
    log.info("[SSJoin] Number of matches " + nm)
    log.info("[SSJoin] Intersection time (s) " + (t3 - t2) / 1000.0)

    val profiles = profiles1.union(profiles2)
    val clusters = ConnectedComponentsClustering.getClusters(profiles, matches.map(x => WeightedEdge(x._1, x._2, 0)), 0)
    clusters.cache()
    val cn = clusters.count()
    val t4 = Calendar.getInstance().getTimeInMillis
    log.info("[SSJoin] Number of clusters " + cn)
    log.info("[SSJoin] Clustering time (s) " + (t4 - t3) / 1000.0)

    log.info("[SSJoin] Total time (s) " + (t4 - t1) / 1000.0)

    val matchedPairs = clusters.map(_._2).flatMap(idSet => {
      val pairs = new ArrayBuffer[(Int, Int)]()
      val idArray = idSet.toArray
      for (i <- 0 until idArray.length) {
        val target: Int = idArray(i)
        for (j <- i + 1 until idArray.length) {
          val source = idArray(j)
          pairs += ((target, source))
        }
      }
      pairs
    })

    val profileMatches = mapMatchesWithProfiles(matchedPairs, profiles)

    val matchesInDiffDataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
    log.info("[SSJoin] Get matched pairs " + matchesInDiffDataSet.count())
    matchesInDiffDataSet.foreach(t => println(
      (t._2, (t._1._1.originalID, t._1._1.sourceId), (t._1._2.originalID, t._1._2.sourceId))))

    log.info("[SSJoin] Completed")
  }

  def mapMatchesWithProfiles(matchedPairs: RDD[(Int, Int)], profiles: RDD[Profile]): RDD[(Profile, Profile)] = {
    val profilesById = profiles.keyBy(_.id)

    matchedPairs.keyBy(_._1).
      join(profilesById).
      map(t => (t._2._1._2, t._2._2)).keyBy(_._1).
      join(profilesById).map(t => (t._2._1._2, t._2._2))
  }

  def getDataType(dataFile: String): String = {
    val theDataFile = dataFile.toLowerCase()
    if (theDataFile.endsWith(".csv")) {
      ProfileLoaderFactory.DATA_TYPE_CSV
    } else if (theDataFile.endsWith(".json")) {
      ProfileLoaderFactory.DATA_TYPE_JSON
    } else if (theDataFile.endsWith(".parquet")) {
      ProfileLoaderFactory.DATA_TYPE_PARQUET
    } else throw new RuntimeException("Do not support this data format")
  }


  def getProfileLoader(dataFile: String): ProfileLoaderTrait = {
    ProfileLoaderFactory.getDataLoader(getDataType(dataFile))
  }

}
