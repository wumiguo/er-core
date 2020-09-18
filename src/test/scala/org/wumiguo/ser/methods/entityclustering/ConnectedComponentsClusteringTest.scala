package org.wumiguo.ser.methods.entityclustering

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile, UnweightedEdge, WeightedEdge}
import org.wumiguo.ser.methods.entityclustering.ConnectedComponentsClustering.{getClusters, getWeightedClusters, getWeightedClustersV2, linkWeightedCluster}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @author levinliu
 *         Created on 2020/7/7
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class ConnectedComponentsClusteringTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "getClusters v1" in {
    val candiPairs = spark.sparkContext.parallelize(Seq(WeightedEdge(11, 12, 0.6), WeightedEdge(12, 1111, 0.4)))
    val prof1 = Profile(11, mutable.MutableList(KeyValue("title", "entity matching test")), "o101", 11)
    val prof2 = Profile(12, mutable.MutableList(KeyValue("title", "entity matching test")), "o102", 12)
    val prof3 = Profile(11111, mutable.MutableList(KeyValue("title", "different entity test")), "o111", 11211)
    val profiles = spark.sparkContext.parallelize(Seq(prof1, prof2, prof3), 2)
    val ccc1 = getClusters(profiles, candiPairs, 0, 0.3)
    val similarPairs1 = ccc1.sortBy(_._2.size, false).collect.toList
    assert(2 == ccc1.count())
    assertResult(List(
      (0, Set(11, 12, 1111)), (11111, Set(11111))
    ))(similarPairs1)
  }

  it should "getClusters v2" in {
    val candiPairs = spark.sparkContext.parallelize(Seq(
      WeightedEdge(11, 12, 0.6), WeightedEdge(12, 1111, 0.4), WeightedEdge(11, 1111, 0.1)
    ))
    val prof1 = Profile(11, mutable.MutableList(KeyValue("title", "entity matching test")), "o101", 1)
    val prof2 = Profile(12, mutable.MutableList(KeyValue("title", "entity matching test")), "o102", 2)
    val prof3 = Profile(1111, mutable.MutableList(KeyValue("title", "entity matching test2")), "o111", 1)
    val prof4 = Profile(11111,
      mutable.MutableList(KeyValue("title", "different entity test"), KeyValue("author", "levin"))
      , "o11211", 2)
    val profiles = spark.sparkContext.parallelize(Seq(prof1, prof2, prof3, prof4), 2)
    val ccc1 = getClusters(profiles, candiPairs, 0, 0.3)
    val similarPairs1 = ccc1.sortBy(_._2.size, false).collect.toList
    assert(2 == ccc1.count())
    assertResult(List(
      (0, Set(11, 12, 1111)), (11111, Set(11111))
    ))(similarPairs1)
    // higher threshold the less item in likelihood scope
    val ccc2 = getClusters(profiles, candiPairs, 0, 0.5)
    val similarPairs2 = ccc2.sortBy(_._2.size, false).collect.toList
    assert(2 == ccc1.count())
    assertResult(List(
      (0, Set(11, 12)), (1111, Set(1111)), (11111, Set(11111))
    ))(similarPairs2)
  }


  it should "getWeightedClusters " in {
    val candiPairs = spark.sparkContext.parallelize(Seq(WeightedEdge(11, 12, 0.6), WeightedEdge(12, 1111, 0.4)))
    val attrs = mutable.MutableList[KeyValue]()
    attrs += KeyValue("title", "entity matching test")
    val prof1 = Profile(11, attrs, "o101", 11)
    val prof2 = Profile(12, attrs, "o101", 12)
    val prof3 = Profile(11111, attrs, "o111", 11211)
    val profiles = spark.sparkContext.parallelize(Seq(prof1, prof2, prof3), 2)
    val ccc1 = getWeightedClusters(profiles, candiPairs, 0, 0.3)
    val similarPairs1 = ccc1.sortBy(_._2.size, false).collect.toList
    assert(1 == ccc1.count())
    assertResult(List(
      (0, ArrayBuffer(
        (11, Map((11, 12) -> 0.6)),
        (12, Map((11, 12) -> 0.6)),
        (12, Map((12, 1111) -> 0.4)),
        (1111, Map((12, 1111) -> 0.4))
      ))
    ))(similarPairs1)
  }

  it should "getWeightedClustersV2 " in {
    val candiPairs = spark.sparkContext.parallelize(Seq(WeightedEdge(11, 12, 0.6), WeightedEdge(12, 1111, 0.4)))
    val attrs = mutable.MutableList[KeyValue]()
    attrs += KeyValue("title", "entity matching test")
    val prof1 = Profile(11, attrs, "o101", 11)
    val prof2 = Profile(12, attrs, "o101", 12)
    val prof3 = Profile(11111, attrs, "o111", 11211)
    val profiles = spark.sparkContext.parallelize(Seq(prof1, prof2, prof3), 2)
    val ccc1 = getWeightedClustersV2(profiles, candiPairs, 0, 0.3)
    val similarPairs1 = ccc1.collect.toList
    assert(1 == ccc1.count())
    assertResult(List((0, (
      Set(11, 12, 12, 1111),
      Map((11, 12) -> 0.6, (12, 1111) -> 0.4)
    )))
    )(similarPairs1)
  }

  it should "linkWeightedCluster " in {
    val candiPairs = spark.sparkContext.parallelize(Seq(WeightedEdge(11, 12, 0.6), WeightedEdge(12, 1111, 0.4)))
    val attrs = mutable.MutableList[KeyValue]()
    attrs += KeyValue("title", "entity matching test")
    val prof1 = Profile(11, attrs, "o101", 11)
    val prof2 = Profile(12, attrs, "o101", 12)
    val prof3 = Profile(11111, attrs, "o111", 11211)
    val profiles = spark.sparkContext.parallelize(Seq(prof1, prof2, prof3), 2)
    val ccc1 = linkWeightedCluster(profiles, candiPairs, 0, 0.3)
    val similarPairs1 = ccc1.collect.toList
    assert(1 == ccc1.count())
    assertResult(List((
      0, (Set(11, 12, 1111),
      Map(
        (11, 12) -> 0.6,
        (11, 1111) -> 0.0,
        (12, 1111) -> 0.4)
    )
    ))
    )(similarPairs1)
  }
}
