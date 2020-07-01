package org.wumiguo.ser.methods.blockrefinement

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.ProfileBlocks

/**
 * @author levinliu
 *         Created on 2020/7/1
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class BlockFilteringTest extends FlatSpec with SparkEnvSetup{
  val spark = createLocalSparkSession(this.getClass.getName)
  it should "filtering block " in {
//    val profileBlocks = spark.sparkContext.parallelize(Seq(ProfileBlocks()))
//    BlockFiltering.blockFiltering()
  }
}
