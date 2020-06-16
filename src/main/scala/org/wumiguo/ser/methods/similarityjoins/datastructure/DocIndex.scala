package org.wumiguo.ser.methods.similarityjoins.datastructure

case class DocIndex(pos: Int, docLen: Int)

//listDocId_tokenPos_numTokens
case class TokenDocumentInfo(docId: Int, pos: Int, docLen: Int)