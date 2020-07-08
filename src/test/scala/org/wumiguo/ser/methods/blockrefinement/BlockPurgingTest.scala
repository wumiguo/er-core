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
  it should "" in {
    val baRdd: RDD[BlockAbstract] = spark.sparkContext.parallelize(
      Seq(BlockDirty(99, Array[Set[Int]](Set[Int](11, 12, 15, 16)), 0.91, 5555))
    )
    val bapRdd = BlockPurging.blockPurging(baRdd, 0.5)
    bapRdd.foreach(x => println("after-purging:" + x))
  }
}
