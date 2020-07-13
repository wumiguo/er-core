package org.wumiguo.ser.methods.blockbuilding

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.dataloader.CSVLoader
import org.wumiguo.ser.methods.datastructure.{BlockClean, BlockDirty, KeyValue, KeysCluster, Profile}
import org.wumiguo.ser.testutil.TestDirs

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class TokenBlockingTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(this.getClass.getName)
  it should " remove black word " in {
    val rdd = spark.sparkContext.parallelize(Seq(
      ("Hello, my team is bigdata", 1),
      ("Hey", 2),
      ("_", 3),
      ("Testing code is nice", 4),
      ("and", 5),
      ("BigData", 6),
      ("IT", 7),
      ("You", 8),
      ("testing", 9),
      ("I", 10)))
    var result = TokenBlocking.removeBadWords(rdd)
    println("remove bad words complete")
    result.foreach(x => println("output is " + x))
    assertResult(6)(result.count())
  }

  it should "create blocks v1" in {
    val attrs1 = mutable.MutableList[KeyValue](KeyValue("title", "helloworld"), KeyValue("author", "lev"))
    val attrs2 = mutable.MutableList[KeyValue](KeyValue("title", "hello world"), KeyValue("author", "liu"))
    val attrs3 = mutable.MutableList[KeyValue](KeyValue("title", "BigData Tech"), KeyValue("author", "liu"))
    val p1 = Profile(1, attrs1, "jaOkd", 100)
    val p2 = Profile(2, attrs2, "Uja2d", 102)
    val p3 = Profile(3, attrs3, "Xlal3", 103)
    val rdd = spark.sparkContext.parallelize(Seq(
      p1, p2, p3
    ))
    println("ep1Rdd size = " + rdd.count())
    val blockRdd = TokenBlocking.createBlocks(rdd)
    println("blocks size = " + blockRdd.count())
    blockRdd.foreach(x => println("block : " + x))
  }

  it should "create blocks" in {
    val rdd = spark.sparkContext.parallelize(Seq(
      ("Hello, my team is bigdata", 1),
      ("Hey", 2),
      ("_", 3),
      ("Testing code is nice", 4),
      ("and", 5),
      ("BigData", 6),
      ("IT", 7),
      ("You", 8),
      ("testing", 9),
      ("I", 10)))
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.csv")
    val startIdFrom = 1
    val ep1Rdd = CSVLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, separator = ",", header = true, realIDField = "year")
    println("ep1Rdd size = " + ep1Rdd.count())
    val blockRdd = TokenBlocking.createBlocks(ep1Rdd)
    println("blocks size = " + blockRdd.count())
    blockRdd.foreach(x => println("block : " + x))
  }

  it should "separateProfiles" in {
    val input = Set[Int](0, 11, 22, 33, 44, 55, 66, 77, 88, 99, 10, 12, 100)
    val separators = Array[Int](11, 33, 55)
    val result = TokenBlocking.separateProfiles(input, separators)
    result.foreach(x => println("result is = " + x))
    assert(4 == result.length)
    val first = result.take(1).head
    assert(Set(0, 10, 11) == first)
  }

  it should "createBlocksCluster " in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.csv")
    val startIdFrom = 1
    val ep1Rdd = CSVLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, separator = ",", header = true, realIDField = "year")
    ep1Rdd.foreach(x => println("ep1Rdd : " + x))
    val separators = Array[Int](11, 33, 55)
    var clusters = List[KeysCluster]()
    clusters = KeysCluster(1000, List("name", "nome")) :: clusters
    val blockRdd = TokenBlocking.createBlocksCluster(ep1Rdd, separators, clusters)
    blockRdd.foreach(x => println("block : " + x))
  }

  it should "createBlocksCluster v2 " in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.csv")
    val startIdFrom = 1
    val ep1Rdd = CSVLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, separator = ",", header = true, realIDField = "year")
    ep1Rdd.foreach(x => println("ep1Rdd : " + x))
    val separators = Array[Int]()
    var clusters = List[KeysCluster]()
    clusters = KeysCluster(1000, List("name", "nome")) :: clusters
    val blockRdd = TokenBlocking.createBlocksCluster(ep1Rdd, separators, clusters)
    blockRdd.foreach(x => println("block : " + x))
  }


  it should "createBlocksCluster from different data set " in {
    val attrs1 = mutable.MutableList[KeyValue](KeyValue("title", "helloworld"), KeyValue("author", "lev"))
    val attrs2 = mutable.MutableList[KeyValue](KeyValue("title", "hello world"), KeyValue("author", "liu"))
    val attrs3 = mutable.MutableList[KeyValue](KeyValue("title", "BigData Tech"), KeyValue("postBy", "liu"))
    val attrs4 = mutable.MutableList[KeyValue](KeyValue("title", "bigdata tech"), KeyValue("author", "liu"))
    val p1 = Profile(1, attrs1, "jaOkd", 100)
    val p2 = Profile(2, attrs2, "Uja2d", 102)
    val p3 = Profile(3, attrs3, "Xlal3", 103)
    val p4 = Profile(4, attrs4, "Ulo04", 104)
    val rdd = spark.sparkContext.parallelize(Seq(
      p1, p2, p3, p4
    ))
    rdd.foreach(x => println("ep1Rdd : " + x))
    val separators = Array[Int]()
    var clusters = List[KeysCluster]()
    clusters = KeysCluster(1000, List("name", "nome")) :: clusters
    clusters = KeysCluster(111, List("title")) :: clusters
    clusters = KeysCluster(22, List("author", "postBy")) :: clusters
    val blockRdd = TokenBlocking.createBlocksCluster(rdd, separators, clusters)
    blockRdd.foreach(x => println("block : " + x))
    assert(blockRdd.count() == 3)
    var data = Array[Set[Int]]()
    data +:= Set[Int](3, 4)
    val exp = BlockDirty(blockID = 0, profiles = data, entropy = 1.0, clusterID = 2222, blockingKey = "bigdata_2222")
    //assertResult(exp)(blockRdd.first())
    val output = blockRdd.sortBy(_.blockingKey, false).first()
    println("comparision ", output.getComparisons())
    assert(exp.clusterID != output.clusterID)
    assert(exp.getComparisons() == output.getComparisons())
  }

  it should "createBlocksCluster v4 with data from same data source(sourceID) " in {
    val attrs1 = mutable.MutableList[KeyValue](KeyValue("title", "helloworld"), KeyValue("author", "lev"))
    val attrs2 = mutable.MutableList[KeyValue](KeyValue("title", "hello world"), KeyValue("author", "liu"))
    val attrs3 = mutable.MutableList[KeyValue](KeyValue("title", "BigData Tech"), KeyValue("postBy", "liu"))
    val attrs4 = mutable.MutableList[KeyValue](KeyValue("title", "bigdata tech"), KeyValue("author", "liu"))
    val p1 = Profile(1, attrs1, "jaOkd", 102)
    val p2 = Profile(2, attrs2, "Uja2d", 102)
    val p3 = Profile(3, attrs3, "Xlal3", 102)
    val p4 = Profile(4, attrs4, "Ulo04", 102)
    val rdd = spark.sparkContext.parallelize(Seq(
      p1, p2, p3, p4
    ))
    rdd.foreach(x => println("ep1Rdd : " + x))
    val separators = Array[Int]()
    var clusters = List[KeysCluster]()
    clusters = KeysCluster(1000, List("name", "nome")) :: clusters
    clusters = KeysCluster(111, List("102_title")) :: clusters
    clusters = KeysCluster(22, List("102_author", "postBy")) :: clusters
    val blockRdd = TokenBlocking.createBlocksCluster(rdd, separators, clusters)
    blockRdd.foreach(x => println("block : " + x))
    assert(blockRdd.count() == 3)
    var data = Array[Set[Int]]()
    data +:= Set[Int](3, 4)
    val exp = BlockDirty(blockID = 0, profiles = data, entropy = 1.0, clusterID = 111, blockingKey = "bigdata_111")
    //assertResult(exp)(blockRdd.first())
    val output = blockRdd.sortBy(_.blockingKey, false).first()
    println("comparision= ", output.getComparisons())
    assert(exp.clusterID == output.clusterID)
    assert(exp.getComparisons() == output.getComparisons())
  }

  it should "createBlocksCluster v6 with data from 2 data source(sourceID) " in {
    val attrs1 = mutable.MutableList[KeyValue](KeyValue("title", "helloworld"), KeyValue("author", "lev"))
    val attrs2 = mutable.MutableList[KeyValue](KeyValue("title", "hello world"), KeyValue("author", "liu"))
    val attrs3 = mutable.MutableList[KeyValue](KeyValue("title", "BigData Tech"), KeyValue("postBy", "liu"))
    val attrs4 = mutable.MutableList[KeyValue](KeyValue("title", "bigdata tech"), KeyValue("author", "liu"))
    val attrs5 = mutable.MutableList[KeyValue](KeyValue("title", "data tech"), KeyValue("author", "levin"))
    val p1 = Profile(1, attrs1, "jaOkd", 102)
    val p2 = Profile(2, attrs2, "Uja2d", 103)
    val p3 = Profile(3, attrs3, "Xlal3", 102)
    val p4 = Profile(4, attrs4, "Ulo04", 103)
    val p5 = Profile(5, attrs5, "Ulo05", 102)
    val rdd = spark.sparkContext.parallelize(Seq(
      p1, p2, p3, p4, p5
    ))
    val separators = Array[Int](6)
    var clusters = List[KeysCluster]()
    clusters = KeysCluster(1000, List("name", "nome")) :: clusters
    clusters = KeysCluster(111, List("102_title")) :: clusters
    clusters = KeysCluster(22, List("102_author", "102_postBy")) :: clusters
    val blockRdd = TokenBlocking.createBlocksCluster(rdd, separators, clusters)
    blockRdd.foreach(x => println("block : " + x))
    assertResult(0)(blockRdd.count())
  }
}
