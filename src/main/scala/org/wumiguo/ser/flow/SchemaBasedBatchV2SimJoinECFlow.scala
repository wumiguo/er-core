package org.wumiguo.ser.flow

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.wumiguo.ser.common.{SparkAppConfigurationSupport, SparkEnvSetup}
import org.wumiguo.ser.datawriter.GenericDataWriter.generateOutputWithSchema
import org.wumiguo.ser.flow.configuration.{CommandLineConfigLoader, FlowOptions}
import org.wumiguo.ser.flow.render.ERResultRender
import org.wumiguo.ser.methods.datastructure.{Profile, WeightedEdge}
import org.wumiguo.ser.methods.entityclustering.ConnectedComponentsClustering
import org.wumiguo.ser.methods.similarityjoins.simjoin.{EDBatchJoin, EDBatchSimpleJoin}
import org.wumiguo.ser.methods.util.CommandLineUtil
import org.wumiguo.ser.methods.util.PrintContext.printSession

import scala.collection.mutable.ArrayBuffer

/**
 * @author levinliu
 *         Created on 2020/9/8
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object SchemaBasedBatchV2SimJoinECFlow extends ERFlow with SparkEnvSetup with SimJoinCommonTrait {

  private val ALGORITHM_EDJOIN = "EDJoin"
  private val ALGORITHM_PARTENUM = "PartEnum"

  override def run(args: Array[String]): Unit = {
    val sparkConf = SparkAppConfigurationSupport.args2SparkConf(args)
    val spark = createSparkSession(getClass.getName, appConf = sparkConf)
    printSession(spark)
    val dataSet1 = CommandLineConfigLoader.load(args, "dataSet1")
    val dataSet2 = CommandLineConfigLoader.load(args, "dataSet2")

    val outputPath = CommandLineUtil.getParameter(args, "outputPath", "output/mapping")
    val outputType = CommandLineUtil.getParameter(args, "outputType", "json")
    val joinResultFile = CommandLineUtil.getParameter(args, "joinResultFile", "mapping")
    val overwriteOnExist = CommandLineUtil.getParameter(args, "overwriteOnExist", "false")
    val showSimilarity = CommandLineUtil.getParameter(args, "showSimilarity", "false")
    val joinFieldsWeight = CommandLineUtil.getParameter(args, "joinFieldsWeight", "")
    log.info("dataSet1=" + dataSet1)
    log.info("dataSet2=" + dataSet2)
    preCheckOnAttributePair(dataSet1, dataSet2)
    val weighted = joinFieldsWeight != null && joinFieldsWeight.trim != ""
    val weightValues = checkAndResolveWeights(joinFieldsWeight, dataSet1)
    preCheckOnWeight(weightValues)


    val profiles1: RDD[Profile] = loadDataWithGivenOptionOnly(dataSet1, 0, 0)
    val numberOfProfile1 = profiles1.count()
    val secondEPStartID = numberOfProfile1.intValue()
    log.info("profiles1 count=" + numberOfProfile1)
    preCheckOnProfile(numberOfProfile1)

    val profiles2: RDD[Profile] = loadDataWithGivenOptionOnly(dataSet2, secondEPStartID, 1)
    val numberOfProfile2 = profiles2.count()
    log.info("profiles2 count=" + numberOfProfile2)
    preCheckOnProfile(numberOfProfile2)

    val flowOptions = FlowOptions.getOptions(args)
    log.info("flowOptions=" + flowOptions)
    val t1 = Calendar.getInstance().getTimeInMillis
    val attributeArrayPair = collectAttributesPairFromProfiles(profiles1, profiles2, dataSet1, dataSet2)
    val matchDetails = doJoin(flowOptions, attributeArrayPair, weighted, weightValues)
    val t2 = Calendar.getInstance().getTimeInMillis

    log.info("[SSJoin] Global join+verification time (s) " + (t2 - t1) / 1000.0)
    val nm = matchDetails.count()
    log.info("[SSJoin] Number of matches " + nm)
    val t3 = Calendar.getInstance().getTimeInMillis
    log.info("[SSJoin] Intersection time (s) " + (t3 - t2) / 1000.0)
    if (nm > 0) {
      log.info("[SSJoin] First matches " + matchDetails.first())
    }
    val profiles = profiles1.union(profiles2)
    val clusters = if (showSimilarity.toBoolean) {
      ConnectedComponentsClustering.linkWeightedCluster(profiles,
        matchDetails.map(x => WeightedEdge(x._1, x._2, x._3)), maxProfileID = 0, edgesThreshold = 0.0)
    } else {
      ConnectedComponentsClustering.getWeightedClustersV2(profiles, matchDetails.map(x => WeightedEdge(x._1, x._2, x._3)), maxProfileID = 0, edgesThreshold = 0.0)
    }
    clusters.cache()
    val cn = clusters.count()
    val t4 = Calendar.getInstance().getTimeInMillis
    log.info("[SSJoin] Number of clusters " + cn)
    log.info("[SSJoin] Clustering time (s) " + (t4 - t3) / 1000.0)
    log.info("[SSJoin] Total time (s) " + (t4 - t1) / 1000.0)
    val connectedLinkageThreshold = flowOptions.getOrElse("relativeLinkageThreshold", "0.0").toDouble
    val matchedPairs: RDD[(Int, Int, Double)] = filterConnectedCluster(clusters, connectedLinkageThreshold)
    if (!matchedPairs.isEmpty()) {
      matchDetails.take(3).foreach(x => log.info("matchDetails=" + x))
      matchedPairs.take(3).foreach(x => log.info("matchedPair=" + x))
    }
    log.info("matchedPairsCount=" + matchedPairs.count() + ",matchDetails=" + matchDetails.count())
    val showSim = showSimilarity.toBoolean
    val (columnNames, rows) = ERResultRender.posLoadThenRenderResult(dataSet1, dataSet2,
      secondEPStartID, matchDetails, profiles, matchedPairs, showSim)
    val overwrite = overwriteOnExist.toBoolean
    val finalPath = generateOutputWithSchema(columnNames, rows, outputPath, outputType, joinResultFile, overwrite)
    log.info("save mapping into path " + finalPath)
    log.info("[SSJoin] Completed")
  }

  def filterConnectedCluster(clusters: RDD[(Int, (Set[Int], Map[(Int, Int), Double]))], connectedLinkageThreshold: Double): RDD[(Int, Int, Double)] = {
    val matchedPairs = clusters.map(_._2).flatMap { case (ids, map) => {
      val pairs = new ArrayBuffer[(Int, Int, Double)]()
      val idArray = ids.toArray
      for (i <- 0 until idArray.length) {
        val target: Int = idArray(i)
        for (j <- i + 1 until idArray.length) {
          val source = idArray(j)
          val score = map.getOrElse((target, source), map.getOrElse((source, target), 10E-5))
          if (score >= connectedLinkageThreshold) {
            pairs += ((target, source, score))
          }
        }
      }
      pairs
    }
    }
    matchedPairs
  }

  def doJoin(flowOptions: Map[String, String], attributeArrayPair: (RDD[(Int, Array[String])], RDD[(Int, Array[String])]),
             weighted: Boolean, weights: List[Double]) = {
    val q = flowOptions.get("q").getOrElse("2")
    val algorithm = flowOptions.get("algorithm").getOrElse(ALGORITHM_EDJOIN)
    val threshold = flowOptions.get("threshold").getOrElse("2")
    val scale = flowOptions.get("scale").getOrElse("3").toInt

    def getMatches(pair: (RDD[(Int, Array[String])], RDD[(Int, Array[String])])): RDD[(Int, Int, Double)] = {
      algorithm match {
        case ALGORITHM_EDJOIN =>
          val attributes = pair._1.union(pair._2)
          EDBatchJoin.getMatchesV2(attributes, q.toInt, threshold.toInt, weighted, weights.zipWithIndex.map(_.swap).toMap)
        case _ => throw new RuntimeException("Unsupported algo " + algorithm)
      }
    }

    var attributesMatches: RDD[(Int, Int, Double)] = getMatches(attributeArrayPair)
    //    if (weighted) {
    //      attributesMatches = attributesMatches.map(x => (x._1, x._2, weights(0) / (x._3 + 1)))
    //    }
    //    for (i <- 1 until attributeArrayPair.length) {
    //      val next = attributePairsArray(i)
    //      if (weighted) {
    //        val nextMatches = getMatches(next).map(x => (x._1, x._2, weights(i) / (x._3 + 1)))
    //        attributesMatches = attributesMatches.union(nextMatches).groupBy(x => (x._1, x._2)).map(x => x._2.reduce((y, z) =>
    //          (y._1, y._2, (BigDecimal(y._3.toString) + BigDecimal(z._3.toString)).setScale(scale, BigDecimal.RoundingMode.HALF_UP).doubleValue())
    //        ))
    //      } else {
    //        attributesMatches = attributesMatches.intersection(getMatches(next))
    //      }
    //    }
    attributesMatches
  }
}
