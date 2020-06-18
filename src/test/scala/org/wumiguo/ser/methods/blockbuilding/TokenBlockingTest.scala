package org.wumiguo.ser.methods.blockbuilding

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup

/**
 * @author levinliu
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class TokenBlockingTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(this.getClass.getName)
  "remove back word " should " remove " in {
    val rdd = spark.sparkContext.parallelize(Seq(
      ("Hello, my team is bigdata", 1),
      ("Hey, nice day", 2),
      ("Testing code is nice", 3)))
    var result = TokenBlocking.removeBadWords(rdd)
    result.foreach(x => println("output is " + x))
  }

}
