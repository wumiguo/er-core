package org.wumiguo.ser.methods.similarityjoins.common.ed

import org.apache.spark.rdd.RDD
import org.wumiguo.ser.methods.similarityjoins.datastructure.Qgram

/**
 * @author levinliu
 *         Created on 2020/6/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object CommonEdFunctions {

  object commons {
    def fixPrefix: (Int, Int) = (-1, -1)
  }

  /**
   * Calculate edit distance
   *
   * @param a
   * @param b
   * @tparam A
   * @return
   */
  def editDist[A](a: Iterable[A], b: Iterable[A]): Int = {
    ((0 to b.size).toList /: a) ((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => math.min(math.min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      }) last
  }

  /**
   * split string into queue of gram
   *
   * @param str
   * @param qgramSize
   * @return
   */
  def getQgrams(str: String, qgramSize: Int): Array[(String, Int)] = {
    str.sliding(qgramSize).zipWithIndex.map(q => (q._1, q._2)).toArray
  }


  /**
   * count the words in the docs' array
   *
   * @param docs
   * @return
   */
  def getQgramsTf(docs: RDD[(Int, Array[(String, Int)])]): Map[String, Int] = {
    //output [se,el,lf,el...]
    val allQgrams = docs.flatMap { case (docId, qgrams) =>
      qgrams.map { case (str, pos) =>
        str
      }
    }
    //output [(se,1),(el,2),(lf,1)...]
    allQgrams.groupBy(x => x).map(x => (x._1, x._2.size)).collectAsMap().toMap
  }

  /**
   * Sort the q-grams within the document by their document frequency
   **/
  def getSortedQgrams(docs: RDD[(Int, Array[(String, Int)])]): RDD[(Int, Array[(Int, Int)])] = {
    val tf = getQgramsTf(docs)
    val tf2 = docs.context.broadcast(tf.toList.sortBy(_._2).zipWithIndex.map(x => (x._1._1, x._2)).toMap)
    docs.map { case (docId, qgrams) =>
      val sortedQgrams = qgrams.map(q => (tf2.value(q._1), q._2)).sortBy(q => q)
      (docId, sortedQgrams)
    }
  }

  def getSortedQgrams2(docs: RDD[(Int, String, Array[(String, Int)])]): RDD[(Int, String, Array[(Int, Int)])] = {
    //output [(be,2),(cd,2),(ks,158)...] not ordered
    val tf = getQgramsTf(docs.map(x => (x._1, x._3)))
    //output {token:token id also index of tokens order by term frequency,...}
    // {"xy":1,"bc":2,"ab":3,...}
    val tf2 = docs.context.broadcast(
      //sort by term frequency increasing(idf decreasing)
      tf.toList.sortBy(_._2).
        zipWithIndex.map(x => (x._1._1, x._2)).toMap)
    docs.map { case (docId, doc, qgrams) =>
      //output [(tokenId,token position of q-gram)]
      //[(617,28).(641,29),(726,25)...]
      val sortedQgrams = qgrams.map(q => (tf2.value(q._1), q._2)).sortBy(q => q)
      (docId, doc, sortedQgrams)
    }
  }

  /**
   * Given the list of documents with the ordered q-grams, create the prefix index.
   * Note: to solve the problem of documents that are too short, the prefix index contains a block identified by the id
   * specified in "fixprefix" which contains all documents that cannot be verified with certainty.
   **/
  def buildPrefixIndex(sortedDocs: RDD[(Int, Array[(Int, Int)])], qgramLen: Int, threshold: Int): RDD[(Int, Array[Qgram])] = {
    val prefixLen = EdFilters.getPrefixLen(qgramLen, threshold)

    val allQgrams = sortedDocs.flatMap { case (docId, qgrams) =>
      val prefix = {
        if (qgrams.length < prefixLen) {
          qgrams.union(commons.fixPrefix :: Nil)
        }
        else {
          qgrams.take(prefixLen)
        }
      }
      prefix.zipWithIndex.map { case (qgram, index) =>
        (qgram._1, Qgram(docId, qgrams.length, qgram._2, index))
      }
    }
    allQgrams.groupBy(_._1).filter(_._2.size > 1).map(x => (x._1, x._2.map(_._2).toArray))
  }
}
