package org.wumiguo.ser.common

import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/9/24
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class SparkAppConfigurationSupportTest extends AnyFlatSpec {
  it should "exchange spark config" in {
    val sparkConf = new SparkAppConfiguration()
    sparkConf.master = "localhost:8080"
    sparkConf.enableHiveSupport = true
    sparkConf.options = mutable.Map()
    sparkConf.options.put("k", "v")
    val args = SparkAppConfigurationSupport.sparkConf2Args(sparkConf)
    assertResult(Array("spark-master=localhost:8080",
      "spark-enableHiveSupport=true",
      "spark-optionSize=1",
      "spark-option0=k:v"))(args)
    val actualConf = SparkAppConfigurationSupport.args2SparkConf(args)
    assertResult(sparkConf)(actualConf)
  }
}
