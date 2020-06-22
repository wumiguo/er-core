package org.wumiguo.ser.methods.blockbuilding

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.dataloader.CSVLoader
import org.wumiguo.ser.methods.datastructure.KeysCluster
import org.wumiguo.ser.testutil.TestDirs

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
    val ep1Rdd = CSVLoader.loadProfiles2(ep1Path, startIdFrom, separator = ",", header = true, realIDField = "year")
    val blockRdd = TokenBlocking.createBlocks(ep1Rdd)
    blockRdd.foreach(x => println("block : " + x))
  }

  it should "separateProfiles" in {
    val input = Set[Int](0, 11, 22, 33, 44, 55, 66, 77, 88, 99, 10, 12, 100)
    val separators = Array[Int](11, 33, 55)
    val result = TokenBlocking.separateProfiles(input, separators)
    result.foreach(x => println("result is = " + x))
    assert(4 == result.length)
  }

  it should "createBlocksCluster " in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.csv")
    val startIdFrom = 1
    val ep1Rdd = CSVLoader.loadProfiles2(ep1Path, startIdFrom, separator = ",", header = true, realIDField = "year")
    ep1Rdd.foreach(x => println("ep1Rdd : " + x))
    val separators = Array[Int](11, 33, 55)
    var clusters = List[KeysCluster]()
    clusters = KeysCluster(1000, List("name", "nome")) :: clusters
    val blockRdd = TokenBlocking.createBlocksCluster(ep1Rdd, separators, clusters)
    blockRdd.foreach(x => println("block : " + x))
  }

}
