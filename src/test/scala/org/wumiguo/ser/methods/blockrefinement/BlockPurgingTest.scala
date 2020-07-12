package org.wumiguo.ser.methods.blockrefinement

import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.{BlockAbstract, BlockDirty}

/**
 * @author levinliu
 *         Created on 2020/7/5
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class BlockPurgingTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)

  it should "sumPrecedentLevels" in {
    var input = Array[(Double, (Double, Double))]()
    input +:= (0.8, (0.1, 0.2))
    input +:= (0.5, (0.21, 0.2))
    input +:= (0.9, (0.71, 0.1))
    val result = BlockPurging.sumPrecedentLevels(input)
    result.foreach(x => println("result=" + x))
    assert(3 == result.size)
    val leftItem = result.last
    assertResult((0.8, (1.02, 0.5)))(leftItem)
  }

  it should "calcMaxComparisonNumber" in {
    var input = Array[(Double, (Double, Double))]()
    input +:= (0.8, (0.1, 0.2))
    input +:= (0.5, (0.2, 0.2))
    input +:= (0.9, (0.7, 0.1))
    val smoothFactor = 0.5
    val max = BlockPurging.calcMaxComparisonNumber(input, smoothFactor)
    assert(0.9 == max)
  }

  it should "blockPurging" in {
    val baRdd: RDD[BlockAbstract] = spark.sparkContext.parallelize(
      Seq(BlockDirty(99, Array[Set[Int]](Set[Int](11, 12, 15, 16)), 0.91, 5555))
    )
    val bapRdd = BlockPurging.blockPurging(baRdd, 0.5)
    assert(1 == bapRdd.count())
    bapRdd.foreach(x => println("after-purging:" + x))
  }
}
