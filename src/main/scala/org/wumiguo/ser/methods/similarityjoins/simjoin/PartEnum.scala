package org.wumiguo.ser.methods.similarityjoins.simjoin

import org.apache.spark.rdd.RDD
import org.wumiguo.ser.methods.datastructure.KeyValue
import org.wumiguo.ser.methods.entitymatching.MatchingFunctions
import org.wumiguo.ser.methods.similarityjoins.datastructure.Category

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object PartEnum {

  private val MAX_LEN = 3300 // the max size of the candidate string
  private val MAX_CATEGORY = 100 // max slice of the string, usually won't reach this limit

  def getHelpers(threshold: Double): ArrayBuffer[Category] = {
    val helper = ArrayBuffer[Category]()
    var len = 1

    breakable {
      for (k <- 0 until MAX_CATEGORY) {
        helper += new Category(len, threshold)
        len = helper(k).e_len + 1
        if (len > MAX_LEN)
          break
      }
    }

    helper
  }

  def tokenize(documents: RDD[(Int, String)]): RDD[(Int, String, Array[String])] = {
    documents.map(t => (t._1, t._2, t._2.toLowerCase.split("[\\W_]")))
  }

  def sizeBasedFiltering(tokens: RDD[(Int, String, Array[String])], helpers: ArrayBuffer[Category]): RDD[(Int, Iterable[(Int, Int, String, Array[String])])] = {
    val categoryAndTokens = tokens.map {
      case token => {
        var categoryIndex = -1
        breakable {
          for (i <- 0 until helpers.length) {
            val helper = helpers(i)
            if (helper.s_len >= token._3.length && helper.e_len <= token._3.length) {
              categoryIndex = i
              break
            }
          }
        }
        //categoryIndex,recordId,attribute,tokens
        (categoryIndex, token._1, token._2, token._3)
      }
    }

    categoryAndTokens.union(categoryAndTokens.map(t => (t._1 - 1, t._2, t._3, t._4))).groupBy(_._1).map(t =>
      //categoryIndex, tokens
      (t._1, t._2)
    ).filter(_._1 > 0)
  }


  def getCandidates(documents: RDD[(Int, String)], threshold: Double): RDD[((Int, Int, String, Array[String]), (Int, Int, String, Array[String]))] = {
    val tokens = tokenize(documents)
    val helpers: ArrayBuffer[Category] = getHelpers(threshold)
    val groupedCandidates = sizeBasedFiltering(tokens, helpers)

    groupedCandidates.join(groupedCandidates).flatMap(t => {
      var count = -1l
      //documents.sparkContext.broadcast(count)
      t._2._1.map(token => {
        count += 1
        t._2._2.zipWithIndex.filter(_._2 > count).map(_._1).map(anotherToken => {
          if (token._2 != anotherToken._2) (token, anotherToken) else null
        }).filter(_ != null)
      }).flatten
    })
  }

  def getMatches(documents1: RDD[(Int, String)], documents2: RDD[(Int, String)], threshold: Double): RDD[(Int, Int, Double)] = {
    val candidates = getCandidates(documents1.union(documents2), threshold)
    candidates.map(t => (t._1._2, t._2._2, MatchingFunctions.jaccardSimilarity(KeyValue(null, t._1._3), KeyValue(null, t._2._3)))).filter(t => t._3 >= threshold)
  }
}
