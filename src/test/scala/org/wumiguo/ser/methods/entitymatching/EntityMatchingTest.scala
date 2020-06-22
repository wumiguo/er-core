package org.wumiguo.ser.methods.entitymatching

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/6/22
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class EntityMatchingTest extends FlatSpec with SparkEnvSetup {

  //  val spark = createLocalSparkSession(getClass.getName)

  it should "group linkage" in {
    var attrs1 = mutable.MutableList[KeyValue](KeyValue("hi", "100"), KeyValue("hello", "200"))
    var attrs2 = mutable.MutableList[KeyValue](KeyValue("hi", "100"), KeyValue("hello", "200"))
    val p1 = Profile(1, attrs1, "jaOkd", 100)
    val p2 = Profile(2, attrs2, "Uja2d", 102)
    val weightedEdge = EntityMatching.groupLinkage(p1, p2, 0.8)
    println("we:" + weightedEdge)
  }
}
