package org.wumiguo.ser.methods.blockrefinement.pruningmethod

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup

/**
 * @author levinliu
 *         Created on 2020/7/13
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class CEPTest extends FlatSpec with SparkEnvSetup {

  it should "calcFreq" in {
    val weights = Array[Double](2.1, 2.2, 2.3, 2.1, 2.5, 2.6)
    val neighbors = Array[Int](0, 2, 3, 4)
    val neighborsNumber: Int = 3 //to get neighbors 0,2,3
    //2.1,2.3,2.1
    val arr = CEP.calcFreq(weights, neighbors, neighborsNumber)
    assertResult(List[(Double, Double)]((2.3, 1.0), (2.1, 2.0)))(arr.toList)
  }

  it should "CEP " in {
    //CEP.calcThreshold()
  }

  it should "CEP " in {
    //    CEP.CEP()
  }
}

