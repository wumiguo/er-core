package org.wumiguo.ser.flow

import org.apache.spark.rdd.RDD
import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.flow.SchemaBasedBatchV2SimJoinECFlow.doJoin
import org.wumiguo.ser.flow.configuration.FlowOptions
import org.wumiguo.ser.methods.datastructure.{Profile, WeightedEdge}
import org.wumiguo.ser.methods.entityclustering.ConnectedComponentsClustering

import scala.collection.mutable.ArrayBuffer

/**
 * @author levinliu
 *         Created on 2020/8/27
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class SchemaBasedBatchV2SimJoinECFlowTest extends AnyFlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "doJoin weighted " in {
    val flowOptions = FlowOptions.getOptions(Array("optionSize=3", "option0=algorithm:EDJoin", "option1=threshold:0", "option2=q:2"))
    var attrPairArray = (
      spark.sparkContext.makeRDD(Seq((1, Array("AE0024")))),
      spark.sparkContext.makeRDD(Seq((5, Array("GAE0024"))))
    )
    val res = doJoin(flowOptions, attrPairArray, true, List(1.0))
    assertResult(true)(res.isEmpty())
    val flowOptions2 = FlowOptions.getOptions(Array("optionSize=3", "option0=algorithm:EDJoin", "option1=threshold:1", "option2=q:2"))
    val res2 = doJoin(flowOptions2, attrPairArray, true, List(1.0))
    assertResult(false)(res2.isEmpty())
    assertResult(List((1, 5, 0.8571428571428571)))(res2.collect.toList)
    attrPairArray = (
      spark.sparkContext.makeRDD(Seq((1, Array("AE0024")))),
      spark.sparkContext.makeRDD(Seq((5, Array("AE0024"))))
    )
    val res3 = doJoin(flowOptions2, attrPairArray, true, List(1.0))
    assertResult(List((1, 5, 1.0)))(res3.collect.toList)
    attrPairArray = (
      spark.sparkContext.makeRDD(Seq((1, Array("AE0024", "DEFG")))),
      spark.sparkContext.makeRDD(Seq((5, Array("AE0024", "DEFF"))))
    )
    val res4 = doJoin(flowOptions2, attrPairArray, true, List(1.0, 0.0))
    assertResult(List((1, 5, 1.0)))(res4.collect.toList)
    val res5 = doJoin(flowOptions2, attrPairArray, true, List(0.5, 0.5))
    assertResult(List((1, 5, 0.875)))(res5.collect.toList)
  }

  it should "doJoin unweighted" in {
    val flowOptions = FlowOptions.getOptions(Array("optionSize=3", "option0=algorithm:EDJoin", "option1=threshold:0", "option2=q:2"))
    val weights = List(1.0)
    val weighted = false
    var attrPairArray = (spark.sparkContext.makeRDD(Seq((1, Array("AE0024")))), spark.sparkContext.makeRDD(Seq((5, Array("GAE0024")))))
    val res = doJoin(flowOptions, attrPairArray, weighted, weights)
    assertResult(true)(res.isEmpty())
    val flowOptions2 = FlowOptions.getOptions(Array("optionSize=3", "option0=algorithm:EDJoin", "option1=threshold:1", "option2=q:2"))
    val res2 = doJoin(flowOptions2, attrPairArray, weighted, weights)
    assertResult(false)(res2.isEmpty())
    assertResult(List((1, 5, 0.8571428571428571)))(res2.collect.toList)
    attrPairArray = (spark.sparkContext.makeRDD(Seq((1, Array("AE0024")))), spark.sparkContext.makeRDD(Seq((5, Array("AE0024")))))
    val res3 = doJoin(flowOptions2, attrPairArray, weighted, weights)
    assertResult(List((1, 5, 1.0)))(res3.collect.toList)
    attrPairArray = (spark.sparkContext.makeRDD(Seq((1, Array("AE0024", "UPGK882")))), spark.sparkContext.makeRDD(Seq((5, Array("AE0024", "UPGK882")))))
    val res4 = doJoin(flowOptions2, attrPairArray, weighted, weights).collect.toList
    assertResult(List((1, 5, 1.0)))(res4)
  }

  it should "doJoin and connected cluster" in {
    val flowOptions0 = FlowOptions.getOptions(Array("optionSize=4", "option0=algorithm:EDJoin", "option1=threshold:0", "option2=q:2", "option3=scale:2"))
    val flowOptions = FlowOptions.getOptions(Array("optionSize=4", "option0=algorithm:EDJoin", "option1=threshold:1", "option2=q:2", "option3=scale:2"))
    val weights = List(0.2, 0.8)
    val weighted = true
    var attrPairArray = (
      spark.sparkContext.makeRDD(Seq((1, Array("AE0023", "UPGK882")), (2, Array("GE0023", "UPGK882")))),
      spark.sparkContext.makeRDD(Seq((25, Array("AE0024", "UPGK882")), (26, Array("GU0023", "GULD882")), (27, Array("AU0022", "Djk2342"))))
    )
    val matchDetails = doJoin(flowOptions, attrPairArray, weighted, weights)
    val res = matchDetails.collect.toList
    assertResult(List(
      (2, 26, 0.16666666666666669),
      (2, 25, 0.8),
      (1, 25, 0.9666666666666668),
      (1, 2, 0.9666666666666668)
    ))(res)
    val profiles1 = spark.sparkContext.makeRDD(Seq(Profile(1), Profile(2)))
    val profiles2 = spark.sparkContext.makeRDD(Seq(Profile(25), Profile(26), Profile(27)))
    val profiles = profiles1.union(profiles2)
    val clusters = ConnectedComponentsClustering.getClusters(profiles,
      matchDetails.map(x => WeightedEdge(x._1, x._2, x._3)), 0, 0.0)
    assertResult(List(
      (0, Set(2, 26, 25, 1)),
      (27, Set(27))
    ))(clusters.collect.toList)
    val weightedClusters = ConnectedComponentsClustering.linkWeightedCluster(profiles, matchDetails.map(x => WeightedEdge(x._1, x._2, x._3)), 0, 0.0)
    assertResult(List(
      (0, (Set(2, 26, 25, 1), Map((26, 1) -> 1.0E-6, (26, 25) -> 1.0E-6, (25, 1) -> 0.9666666666666668, (2, 1) -> 0.9666666666666668, (2, 26) -> 0.16666666666666669, (2, 25) -> 0.8)))
    ))(weightedClusters.collect.toList)
  }
}
