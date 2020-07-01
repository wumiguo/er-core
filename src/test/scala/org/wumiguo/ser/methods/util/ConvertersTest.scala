package org.wumiguo.ser.methods.util

import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.{BlockAbstract, BlockDirty, BlockWithComparisonSize, ProfileBlocks}

/**
 * @author levinliu
 *         Created on 2020/7/1
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class ConvertersTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(this.getClass.getName)

  it should "blockIDProfileIDFromBlock" in {
    val aBlock: BlockAbstract = BlockDirty(99, Array[Set[Int]](Set[Int](11, 12, 15)), 0.91, 5555)
    val profileBlocks = Converters.blockIDProfileIDFromBlock(aBlock)
    profileBlocks.foreach(x => println("bwc:" + x))
    val first = profileBlocks.toList.sortBy(_._1).head
    assertResult((11, BlockWithComparisonSize(99, 6.0)))(first)
  }

  it should "blocksToProfileBlocks" in {
    val blocks: RDD[BlockAbstract] = spark.sparkContext.parallelize(Seq(BlockDirty(99, Array[Set[Int]](Set[Int](11, 12, 15, 16)), 0.91, 5555)))
    val profileBlocks = Converters.blocksToProfileBlocks(blocks)
    profileBlocks.foreach(x => println("pb:" + x))
    val sorted = profileBlocks.sortBy(_.profileID)
    assertResult(ProfileBlocks(11, Set(BlockWithComparisonSize(99, 12.0))))(sorted.first())
  }
}
