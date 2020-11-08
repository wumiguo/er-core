package org.wumiguo.ser.flow

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.wumiguo.ser.common.{SparkAppConfigurationSupport, SparkEnvSetup}
import org.wumiguo.ser.datawriter.GenericDataWriter.generateOutputWithSchema
import org.wumiguo.ser.flow.configuration.{CommandLineConfigLoader, FlowOptions}
import org.wumiguo.ser.flow.render.ERResultRender
import org.wumiguo.ser.methods.datastructure.{Profile, WeightedEdge}
import org.wumiguo.ser.methods.entityclustering.ConnectedComponentsClustering
import org.wumiguo.ser.methods.similarityjoins.simjoin.{EDJoin, PartEnum}
import org.wumiguo.ser.methods.util.CommandLineUtil
import org.wumiguo.ser.methods.util.PrintContext.printSession

import scala.collection.mutable.ArrayBuffer

/**
 * @author johnli
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object SchemaBasedSimJoinECPreloadFlow extends ERFlow with SparkEnvSetup with SimJoinCommonTrait {

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


    val profiles1: RDD[Profile] = loadDataInOneGo(dataSet1, 0, 0)
    val numberOfProfile1 = profiles1.count()
    val secondEPStartID = numberOfProfile1.intValue()
    log.info("profiles1 count=" + numberOfProfile1)

    val profiles2: RDD[Profile] = loadDataInOneGo(dataSet2, secondEPStartID, 1)
    log.info("profiles2 count=" + profiles2.count())
    preCheckOnProfile(profiles1)
    preCheckOnProfile(profiles2)

    log.info("profiles1 first=" + profiles1.first())
    log.info("profiles2 first=" + profiles2.first())
    val flowOptions = FlowOptions.getOptions(args)
    log.info("flowOptions=" + flowOptions)
    val t1 = Calendar.getInstance().getTimeInMillis
    val attributePairsArray = collectAttributesFromProfiles(profiles1, profiles2, dataSet1, dataSet2)
    val matchDetails = doJoin(flowOptions, attributePairsArray, weighted, weightValues)
    val t2 = Calendar.getInstance().getTimeInMillis

    log.info("[SSJoin] Global join+verification time (s) " + (t2 - t1) / 1000.0)
    log.info("[SSJoin] match attribute pairs " + attributePairsArray.length)
    val nm = matchDetails.count()
    log.info("[SSJoin] Number of matches " + nm)
    val t3 = Calendar.getInstance().getTimeInMillis
    attributePairsArray.foreach(at => {
      Option(at._1).map(_.unpersist())
      Option(at._2).map(_.unpersist())
    })
    log.info("[SSJoin] Intersection time (s) " + (t3 - t2) / 1000.0)
    if (nm > 0) {
      log.info("[SSJoin] First matches " + matchDetails.first())
    }
    val profiles = profiles1.union(profiles2)
    val connectedClustering = CommandLineUtil.getParameter(args, "connectedClustering", "false").toBoolean
    val matchedPairs = resolveMatchedPairs(connectedClustering, profiles, matchDetails, t3)

    val t4 = Calendar.getInstance().getTimeInMillis
    log.info("[SSJoin] Total time (s) " + (t4 - t1) / 1000.0)
    log.info("matchedPairsCount=" + matchedPairs.count() + ",matchDetails=" + matchDetails.count())
    val showSim = showSimilarity.toBoolean
    val (columnNames, rows) = ERResultRender.renderResultWithPreloadProfiles(dataSet1, dataSet2,
      secondEPStartID, matchDetails, profiles, matchedPairs, showSim, profiles1, profiles2)
    val overwrite = overwriteOnExist.toBoolean
    val finalPath = generateOutputWithSchema(columnNames, rows, outputPath, outputType, joinResultFile, overwrite)
    log.info("save mapping into path " + finalPath)
    log.info("[SSJoin] Completed")
  }

  def resolveMatchedPairs(connectedClustering: Boolean, profiles: RDD[Profile], matchDetails: RDD[(Int, Int, Double)], startTimeStamp: Long): RDD[(Int, Int)] = {
    if (connectedClustering) {
      val clusters = ConnectedComponentsClustering.getClusters(profiles,
        matchDetails.map(x => WeightedEdge(x._1, x._2, x._3)), maxProfileID = 0, edgesThreshold = 0.0)
      clusters.cache()
      val cn = clusters.count()
      val t4 = Calendar.getInstance().getTimeInMillis
      log.info("[SSJoin] Number of clusters " + cn)
      log.info("[SSJoin] Clustering time (s) " + (t4 - startTimeStamp) / 1000.0)
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
      if (!matchedPairs.isEmpty()) {
        matchDetails.take(3).foreach(x => log.info("matchDetails=" + x))
        matchedPairs.take(3).foreach(x => log.info("matchedPair=" + x))
      }
      matchedPairs
    } else {
      matchDetails.map(x => (x._1, x._2))
    }
  }

  def doJoin(flowOptions: Map[String, String], attributePairsArray: ArrayBuffer[(RDD[(Int, String)], RDD[(Int, String)])],
             weighted: Boolean, weights: List[Double]) = {
    if (weighted && weights.size != attributePairsArray.size) {
      throw new RuntimeException("Mismatch weights size " + weights.size + " vs attributePairArray size " + attributePairsArray.size)
    }
    val q = flowOptions.get("q").getOrElse("2")
    val algorithm = flowOptions.get("algorithm").getOrElse(ALGORITHM_EDJOIN)
    val threshold = flowOptions.get("threshold").getOrElse("2")
    val scale = flowOptions.get("scale").getOrElse("3").toInt

    def getMatches(pair: (RDD[(Int, String)], RDD[(Int, String)])): RDD[(Int, Int, Double)] = {
      algorithm match {
        case ALGORITHM_EDJOIN =>
          val attributes = pair._1.union(pair._2)
          EDJoin.getMatches(attributes, q.toInt, threshold.toInt)
        case ALGORITHM_PARTENUM =>
          val attributes = pair._1.union(pair._2)
          PartEnum.getMatches(attributes, threshold.toDouble)
      }
    }

    var attributesMatches: RDD[(Int, Int, Double)] = getMatches(attributePairsArray(0))
    if (weighted) {
      attributesMatches = attributesMatches.map(x => (x._1, x._2, weights(0) / (x._3 + 1)))
    }
    for (i <- 1 until attributePairsArray.length) {
      val next = attributePairsArray(i)
      if (weighted) {
        val nextMatches = getMatches(next).map(x => (x._1, x._2, weights(i) / (x._3 + 1)))
        attributesMatches = attributesMatches.union(nextMatches).groupBy(x => (x._1, x._2)).map(x => x._2.reduce((y, z) =>
          (y._1, y._2, (BigDecimal(y._3.toString) + BigDecimal(z._3.toString)).setScale(scale, BigDecimal.RoundingMode.HALF_UP).doubleValue())
        ))
      } else {
        attributesMatches = attributesMatches.intersection(getMatches(next))
      }
    }
    attributesMatches
  }
}
