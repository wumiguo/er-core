package org.wumiguo.ser.methods.entityclustering

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.{Profile, WeightedEdge}

/**
 * @author levinliu
 *         Created on 2020/7/8
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class EntityClusterUtilsTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "connectedComponents " in {
    var data = Seq(
      WeightedEdge(0, 0, 0.058), WeightedEdge(1, 0, 0.081), WeightedEdge(2, 0, 0.069),
      WeightedEdge(0, 1, 0.625), WeightedEdge(1, 1, 0.055), WeightedEdge(2, 1, 0.073),
      WeightedEdge(0, 2, 0.037), WeightedEdge(1, 2, 0.066), WeightedEdge(2, 2, 0.027))
    val weRdd = spark.sparkContext.parallelize(data)
    val connected = EntityClusterUtils.connectedComponents(weRdd)
    assertResult(List(Seq(
      (0, 0, 0.058), (1, 0, 0.081), (2, 0, 0.069),
      (0, 1, 0.625), (1, 1, 0.055), (2, 1, 0.073),
      (0, 2, 0.037), (1, 2, 0.066), (2, 2, 0.027)
    )))(connected.collect.toList.map(_.toSeq))
    data :+= WeightedEdge(5, 4, 0.919)
    data :+= WeightedEdge(4, 5, 0.812)
    val weRdd2 = spark.sparkContext.parallelize(data)
    val connected2 = EntityClusterUtils.connectedComponents(weRdd2)
    assertResult(List(Seq(
      (0, 0, 0.058), (1, 0, 0.081), (2, 0, 0.069),
      (0, 1, 0.625), (1, 1, 0.055), (2, 1, 0.073),
      (0, 2, 0.037), (1, 2, 0.066), (2, 2, 0.027)
    ), Seq((5, 4, 0.919), (4, 5, 0.812))))(connected2.collect.toList.map(_.toSeq))
    data :+= WeightedEdge(9, 4, 0.442)
    data :+= WeightedEdge(11, 100, 0.721)
    val weRdd3 = spark.sparkContext.parallelize(data)
    val connected3 = EntityClusterUtils.connectedComponents(weRdd3)
    assertResult(List(Seq(
      (0, 0, 0.058), (1, 0, 0.081), (2, 0, 0.069),
      (0, 1, 0.625), (1, 1, 0.055), (2, 1, 0.073),
      (0, 2, 0.037), (1, 2, 0.066), (2, 2, 0.027)
    ), Seq((5, 4, 0.919), (4, 5, 0.812), (9, 4, 0.442)),
      Seq((11, 100, 0.721))))(connected3.collect.toList.map(_.toSeq))
  }

  it should "addUnclusteredProfiles " in {
    val pRdd = spark.sparkContext.parallelize(Seq(
      Profile(1),
      Profile(2),
      Profile(3),
      Profile(4),
      Profile(5)
    ))
    val clusterRdd = spark.sparkContext.parallelize(Seq(
      (0, Set(1, 2, 3))
    ))
    val connected = EntityClusterUtils.addUnclusteredProfiles(pRdd, clusterRdd)
    assertResult(
      List((4, Set(4)), (5, Set(5)), (0, Set(1, 2, 3)))
    )(connected.sortBy(_._2.size).collect.toList)
  }
}
