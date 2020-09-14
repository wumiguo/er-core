package org.wumiguo.ser.flow

import org.apache.spark.rdd.RDD
import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.flow.configuration.FlowOptions
import org.wumiguo.ser.flow.SchemaBasedBatchSimJoinECFlow.doJoin

import scala.collection.mutable.ArrayBuffer

/**
 * @author levinliu
 *         Created on 2020/8/27
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class SchemaBasedBatchSimJoinECFlowTest extends AnyFlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "doJoin" in {
    val flowOptions = FlowOptions.getOptions(Array("optionSize=3", "option0=algorithm:EDJoin", "option1=threshold:0", "option2=q:2"))
    val weights = List(1.0)
    var attrPairArray: (RDD[(Int, Array[String])], RDD[(Int, Array[String])]) =
      (spark.sparkContext.makeRDD(Seq((1, Array("AE0024")))), spark.sparkContext.makeRDD(Seq((5, Array("GAE0024")))))
    val res = doJoin(flowOptions, attrPairArray, true, weights)
    assertResult(true)(res.isEmpty())
    val flowOptions2 = FlowOptions.getOptions(Array("optionSize=3", "option0=algorithm:EDJoin", "option1=threshold:1", "option2=q:2"))
    val res2 = doJoin(flowOptions2, attrPairArray, true, weights)
    assertResult(false)(res2.isEmpty())
    assertResult(List((1, 5, 1.0)))(res2.collect.toList)
    attrPairArray = (spark.sparkContext.makeRDD(Seq((1, Array("AE0024")))), spark.sparkContext.makeRDD(Seq((5, Array("AE0024")))))
    val res3 = doJoin(flowOptions2, attrPairArray, true, weights)
    assertResult(List((1, 5, 0.0)))(res3.collect.toList)
    attrPairArray = (spark.sparkContext.makeRDD(Seq((1, Array("AE0024", "PU001")))), spark.sparkContext.makeRDD(Seq((5, Array("AE0024", "PA001")))))
    val res4 = doJoin(flowOptions2, attrPairArray, true, weights)
    assertResult(List((1, 5, 1.0)))(res4.collect.toList)
    attrPairArray = (spark.sparkContext.makeRDD(Seq((1, Array("AI0024", "PU001")))), spark.sparkContext.makeRDD(Seq((5, Array("AE0024", "PA001")))))
    val res5 = doJoin(flowOptions2, attrPairArray, true, weights)
    assertResult(List((1, 5, 2.0)))(res5.collect.toList)
    attrPairArray = (
      spark.sparkContext.makeRDD(Seq((1, Array("AI0024", "PU001", "XG9213")))),
      spark.sparkContext.makeRDD(Seq((5, Array("AE0024", "PA001", "XG8213")))))
    val res6 = doJoin(flowOptions2, attrPairArray, true, weights)
    assertResult(List((1, 5, 3.0)))(res6.collect.toList)
    attrPairArray = (
      spark.sparkContext.makeRDD(Seq((1, Array("AI0024", "PU001", "XG9213")))),
      spark.sparkContext.makeRDD(Seq((5, Array("AE0024", "PA001", "XG8203")))))
    val res7 = doJoin(flowOptions2, attrPairArray, true, weights)
    assertResult(List())(res7.collect.toList)
    attrPairArray = (
      spark.sparkContext.makeRDD(
        Seq((1, Array("AI0024", "PU001", "XG9213")))),
      spark.sparkContext.makeRDD(Seq(
        (5, Array("AE0024", "PA001", "XG8203")),
        (6, Array("AE0024", "PA001", "XG9203")))))
    val res8 = doJoin(flowOptions2, attrPairArray, true, weights)
    assertResult(List((1, 6, 3.0), (5, 6, 1.0)))(res8.collect.toList)
  }
}
