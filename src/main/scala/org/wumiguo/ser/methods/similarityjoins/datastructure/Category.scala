package org.wumiguo.ser.methods.similarityjoins.datastructure

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Category {

  var THRESHOLD: Float = null
  var N1: Int = null
  var N2: Int = null
  var K: Int = null
  var K2: Int = null
  var range_start: ArrayBuffer[ArrayBuffer[Int]] = new ArrayBuffer[ArrayBuffer[Int]]()
  var range_end: ArrayBuffer[ArrayBuffer[Int]] = new ArrayBuffer[ArrayBuffer[Int]]()

  var s_len = 0
  var e_len = 0

  var sig_len = 0

  var subs: ArrayBuffer[ArrayBuffer[Int]] = new ArrayBuffer[ArrayBuffer[Int]]
  var sig_map: Array[mutable.HashMap[Integer, ArrayBuffer[Int]]] = null

  def this(len: Int, threshold: Float) {
    this()
    THRESHOLD = threshold
    s_len = len // the start of the this subset of string
    e_len = (s_len / THRESHOLD).toInt // the end of the this subset of string
    K = (2 * (1 - THRESHOLD) / (1 + THRESHOLD) * e_len.toFloat).toInt
    N1 = K + 1
    N2 = 2

    K2 = (K + 1) / N1 - 1

    if ((K + 1) % N1 != 0) {
      K2 += 1
    }

    if (N1 > K + 1 || N1 * N2 <= K + 1) {
      throw new RuntimeException("N1 * N2 is not more than K + 1,consider to adjust the N1 and N2")
    }

    val n: Int = N2
    val k: Int = N2 - K2

    var s = 0
    s = 0
    var sub = new ArrayBuffer[Int]()
    while ( {
      s < k
    }) {
      sub += s
      s += 1
    }
    subs += sub

    sig_len = N1 * subs.size
    sig_map = new Array[mutable.HashMap[Integer, ArrayBuffer[Int]]](sig_len)

    for (is <- 0 until sig_len) {
      sig_map(is) = new mutable.HashMap[Integer, ArrayBuffer[Int]]
    }
  }


}
