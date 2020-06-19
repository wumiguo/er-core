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
    //TokenBlocking.createBlocks(rdd)
  }

}
