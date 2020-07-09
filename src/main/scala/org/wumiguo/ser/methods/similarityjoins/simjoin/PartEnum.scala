package org.wumiguo.ser.methods.similarityjoins.simjoin

import org.apache.spark.rdd.RDD
import org.wumiguo.ser.methods.similarityjoins.common.js.CommonJsFunctions
import org.wumiguo.ser.methods.similarityjoins.datastructure.Category

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object PartEnum {

  private val MAX_LEN = 3300 // the max size of the candidate string
  private val MAX_CATEGORY = 100 // max slice of the string , won't reach this number usually

  def convertToSignature() = ???

  def getHelpers(threshold: Float) = {
    val helper = ArrayBuffer[Category]()
    var len = 1
    val categoryN = 0

    breakable {
      for (k <- 0 until MAX_CATEGORY) {
        helper(k) = new Category(len, threshold)
        len = helper(k).e_len + 1
        if (len > MAX_LEN)
          break
      }
    }

    helper
  }

  def tokenize(documents: RDD[(Int, String)]): Unit = {
    CommonJsFunctions.tokenizeAndSort(documents)
  }


  def getCandidates(documents: RDD[(Int, String)], threadhold: Float): RDD[((Int, String), (Int, String))] = {
    var tokens = tokenize(documents)
    var helper = getHelpers(threadhold)

    null
  }

  def getMatches(documents1: RDD[(Int, String)], documents2: RDD[(Int, String)], threadhold: Float): RDD[(Int, Int)] = {
    var candidates = getCandidates(documents1.union(documents2), threadhold)


    null
  }
}
