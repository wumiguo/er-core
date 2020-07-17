package org.wumiguo.ser.flow

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.dataloader.{DataType, DataTypeResolver, JSONWrapper, ProfileLoaderFactory, ProfileLoaderTrait}
import org.wumiguo.ser.entity.parameter.DataSetConfig
import org.wumiguo.ser.flow.SchemaBasedSimJoinECFlow.log
import org.wumiguo.ser.methods.datastructure.{Profile, WeightedEdge}
import org.wumiguo.ser.methods.entityclustering.ConnectedComponentsClustering
import org.wumiguo.ser.methods.similarityjoins.common.CommonFunctions
import org.wumiguo.ser.methods.similarityjoins.simjoin.{EDJoin, PartEnum}
import org.wumiguo.ser.methods.util.CommandLineUtil

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File

/**
 * @author johnli
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object SchemaBasedSimJoinECFlow extends ERFlow with SparkEnvSetup {

  private val ALGORITHM_EDJOIN = "EDJoin"
  private val ALGORITHM_PARTENUM = "PartEnum"

  override def run(args: Array[String]): Unit = {
    val outputDir: File = File("/tmp/data-er")
    if (!outputDir.exists) {
      outputDir.createDirectory(true)
    }
    val spark = createLocalSparkSession(getClass.getName, outputDir = outputDir.path)
    val dataSet1Path = CommandLineUtil.getParameter(args, "dataSet1", "datasets/clean/DblpAcm/dataset1.json")
    val dataSet1Format = CommandLineUtil.getParameter(args, "dataSet1-format", "json")
    val dataSet1Id = CommandLineUtil.getParameter(args, "dataSet1-id", "realProfileID")
    val attributes1 = CommandLineUtil.getParameter(args, "attributeSet1", "title")
    val dataSet2Path = CommandLineUtil.getParameter(args, "dataSet2", "datasets/clean/DblpAcm/dataset2.json")
    val dataSet2Format = CommandLineUtil.getParameter(args, "dataSet2-format", "json")
    val dataSet2Id = CommandLineUtil.getParameter(args, "dataSet2-id", "realProfileID")
    val attributes2 = CommandLineUtil.getParameter(args, "attributeSet2", "title")
    val q = CommandLineUtil.getParameter(args, "q", "2")
    val threshold = CommandLineUtil.getParameter(args, "threshold", "2")

    val algorithm = CommandLineUtil.getParameter(args, "algorithm", ALGORITHM_EDJOIN)

    val dataSet1 = new DataSetConfig(dataSet1Path, dataSet1Format, dataSet1Id,
      Option(attributes1).map(_.split(",")).orNull)
    val dataSet2 = new DataSetConfig(dataSet2Path, dataSet2Format, dataSet2Id,
      Option(attributes2).map(_.split(",")).orNull)

    def profileLoader: ProfileLoaderTrait = getProfileLoader(dataSet1.path)

    log.debug("resolve profile loader " + profileLoader)

    val profiles1 = profileLoader.load(dataSet1.path, realIDField = dataSet1.dataSetId, startIDFrom = 0, sourceId = 0)
    val profiles2 = profileLoader.load(dataSet2.path, realIDField = dataSet2.dataSetId, startIDFrom = profiles1.count().intValue(), sourceId = 1)
    preCheckOnProfile(profiles1)
    preCheckOnProfile(profiles2)
    log.info("profiles1 first=" + profiles1.first())
    log.info("profiles2 first=" + profiles2.first())

    assert(dataSet2.path == null || dataSet1.attributes.length == dataSet2.attributes.length,
      "If dataSet 2 exist, the number of attribute use to compare between dataSet 1 and dataSet 2 should be equal")

    val attributePairsArray = collectAttributesFromProfiles(profiles1, profiles2, dataSet1, dataSet2)

    val t1 = Calendar.getInstance().getTimeInMillis
    var attributesMatches = new ArrayBuffer[RDD[(Int, Int, Double)]]()
    var attributeses = ArrayBuffer[RDD[(Int, String)]]()
    attributePairsArray.foreach(attributesTuple => {
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
    attributePairsArray.foreach(attributesTuple => {
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

    val matchesInDiffdataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
    log.info("[SSJoin] Get matched pairs " + matchesInDiffdataSet.count())
    matchesInDiffdataSet.foreach(t => println(
      (t._2, (t._1._1.originalID, t._1._1.sourceId), (t._1._2.originalID, t._1._2.sourceId))))

    log.info("[SSJoin] Completed")
  }

  private def preCheckOnProfile(profiles: RDD[Profile]) = {
    val pCount = profiles.count()
    if (pCount <= 0) {
      throw new RuntimeException("Empty profile data set")
    }
    log.info("profiles count=" + pCount)
  }

  def collectAttributesFromProfiles(profiles1: RDD[Profile], profiles2: RDD[Profile], dataSet1: DataSetConfig, dataSet2: DataSetConfig): ArrayBuffer[(RDD[(Int, String)], RDD[(Int, String)])] = {
    var attributesArray = new ArrayBuffer[(RDD[(Int, String)], RDD[(Int, String)])]()
    log.info("dataSet1Attr=" + dataSet1.attributes.toList)
    log.info("dataSet2Attr=" + dataSet2.attributes.toList)
    for (i <- 0 until dataSet1.attributes.length) {
      val attributes1 = CommonFunctions.extractField(profiles1, dataSet1.attributes(i))
      val attributes2 = Option(dataSet2.attributes).map(attributes => CommonFunctions.extractField(profiles2, attributes(i))).orNull
      attributesArray += ((attributes1, attributes2))
    }
    log.info("attributesArray count=" + attributesArray.length)
    log.info("attributesArray _1count=" + attributesArray.head._1.count() + ", _2count=" + attributesArray.head._2.count())
    attributesArray
  }


  def mapMatchesWithProfiles(matchedPairs: RDD[(Int, Int)], profiles: RDD[Profile]): RDD[(Profile, Profile)] = {
    val profilesById = profiles.keyBy(_.id)

    matchedPairs.keyBy(_._1).
      join(profilesById).
      map(t => (t._2._1._2, t._2._2)).keyBy(_._1).
      join(profilesById).map(t => (t._2._1._2, t._2._2))
  }


  def getProfileLoader(dataFile: String): ProfileLoaderTrait = {
    ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(dataFile))
  }

}
