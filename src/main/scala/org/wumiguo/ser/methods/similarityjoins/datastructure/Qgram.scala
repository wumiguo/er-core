package org.wumiguo.ser.methods.similarityjoins.datastructure

/**
  * Rappresenta un qgramma in un documento
  **/
case class Qgram(docId: Int, docLength: Int, qgramPos: Int, sortedPos: Int)
