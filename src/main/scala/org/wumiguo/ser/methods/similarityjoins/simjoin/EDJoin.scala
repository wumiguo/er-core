package org.wumiguo.ser.methods.similarityjoins.simjoin

import java.util.Calendar

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.wumiguo.ser.methods.datastructure.EDJoinPrefixIndexPartitioner
import org.wumiguo.ser.methods.similarityjoins.common.ed.{CommonEdFunctions, EdFilters}

/**
 * @author levinliu
 *         Created on 2020/6/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 *
 *         PRELIMINARIES
 *         1.
 *         A q-gram is a contiguous substring of length q; and its starting position in a string is called its position or location.
 *         A positional q-gram is a q-gram together with its position, usually represented in the form of (token,pos)
 *
 * 2.Count Filtering mandates that s and t must share at least LBs;t = (max(|s|,|t|) − q + 1) − q · τ common q-grams.
 *
 * 3.Length Filtering mandates that ||s| − |t|| ≤ τ.
 */
object EDJoin {
  /**
   * Do prefix-filtering
   *
   * since the blocks all has at least one token common in the prefix,
   * the prefix filtering is completed in here while doing the groupByKey
   */
  def buildPrefixIndex(sortedDocs: RDD[(Int, String, Array[(Int, Int)])], qgramLen: Int, threshold: Int): RDD[(Int, Array[(Int, Int, Array[(Int, Int)], String)])] = {
    val prefixLen = EdFilters.getPrefixLen(qgramLen, threshold)

    //output [(tokenId,(docId,index of q-gram,q-grams/*tokenId,token index of q-gram */,string)...)]
    val allQgrams = sortedDocs.flatMap { case (docId, doc, qgrams) =>
      val prefix = qgrams.take(prefixLen)
      prefix.zipWithIndex.map { case (qgram, index) =>
        (qgram._1, (docId, index, qgrams, doc))
      }
    }

    //output [(tokenId,[strings share the same token])...],and filter the token only be owned by one string
    val blocks = allQgrams.groupByKey().filter(_._2.size > 1)
    blocks.map(b => (b._1, b._2.toArray.sortBy(_._3.length)))
  }

  /**
   * Returns true if the token of the current block is the last common token
   *
   * @param doc1Tokens   tokens of the first document
   * @param doc2Tokens   tokens of the second document
   * @param currentToken id of the current block in which the documents co-occurs
   *
   */
  def isLastCommonTokenPosition(doc1Tokens: Array[(Int, Int)], doc2Tokens: Array[(Int, Int)], currentToken: Int, qgramLen: Int, threshold: Int): Boolean = {
    val prefixLen = EdFilters.getPrefixLen(qgramLen, threshold)
    var d1Index = math.min(doc1Tokens.length - 1, prefixLen - 1)
    var d2Index = math.min(doc2Tokens.length - 1, prefixLen - 1)
    var valid = true
    var continue = true

    /**
     * Starting from the prefix looking for the last common token
     * One exists for sure
     **/
    while (d1Index >= 0 && d2Index >= 0 && continue) {
      /**
       * Common token
       **/
      if (doc1Tokens(d1Index)._1 == doc2Tokens(d2Index)._1) {
        /**
         * If the token is the same of the current block, stop the process
         **/
        if (currentToken == doc1Tokens(d1Index)._1) {
          continue = false
        }
        else {
          /**
           * If it is different, it is not considered valid: needed to avoid to emit duplicates
           **/
          continue = false
          valid = false
        }
      }

      /**
       * Decrement the indexes (note: the tokens are sorted)
       **/
      else if (doc1Tokens(d1Index)._1 > doc2Tokens(d2Index)._1) {
        d1Index -= 1
      }
      else {
        d2Index -= 1
      }
    }
    valid
  }


  def getCandidatePairs(prefixIndex: RDD[(Int, Array[(Int, Int, Array[(Int, Int)], String)])], qgramLength: Int, threshold: Int): RDD[((Int, String), (Int, String))] = {
    /**
     * Repartitions the blocks of the index based on the number of maximum comparisons involved by each block
     */
    val customPartitioner = new EDJoinPrefixIndexPartitioner(prefixIndex.getNumPartitions)
    val repartitionIndex = prefixIndex.map(_.swap).sortBy(x => -(x._1.length * (x._1.length - 1))).partitionBy(customPartitioner)

    repartitionIndex.flatMap { case (
      block /*string contain same token in the prefix*/ ,
      blockId /*tokenId*/ ) =>
      val results = new scala.collection.mutable.HashSet[((Int, String), (Int, String))]()

      var i = 0
      while (i < block.length) {
        var j = i + 1
        val d1Id = block(i)._1 // docId
        val d1Pos = block(i)._2 // index of the prefix
        val d1Qgrams = block(i)._3
        val d1 = block(i)._4 //the string 1

        while (j < block.length) {
          val d2Id = block(j)._1 // docId
          val d2Pos = block(j)._2 // index of the prefix
          val d2Qgrams = block(j)._3
          val d2 = block(j)._4 //the string 2

          if (d1Id != d2Id &&
            //make sure each string pair will only be do the common filter once(which consume lots of compute resource)
            isLastCommonTokenPosition(d1Qgrams, d2Qgrams, blockId, qgramLength, threshold) &&
            math.abs(d1Pos - d2Pos) <= threshold &&
            //length filtering
            math.abs(d1Qgrams.length - d2Qgrams.length) <= threshold
          ) {
            if (EdFilters.commonFilter(d1Qgrams, d2Qgrams, qgramLength, threshold)) {
              //avoid add duplicated pair
              if (d1Id < d2Id) {
                results.add(((d1Id, d1), (d2Id, d2)))
              }
              else {
                results.add(((d2Id, d2), (d1Id, d1)))
              }
            }
          }
          j += 1
        }
        i += 1
      }
      results
    }
  }

  def getPositionalQGrams(documents: RDD[(Int, String)], qgramLength: Int): RDD[(Int, String, Array[(String, Int)])] = {
    documents.map(x => (x._1, x._2, CommonEdFunctions.getQgrams(x._2, qgramLength)))
  }

  def getCandidates(documents: RDD[(Int, String)], qgramLength: Int, threshold: Int): RDD[((Int, String), (Int, String))] = {
    //Transforms the documents into n-grams
    //output example
    //(docId,string,positional q-gram)
    //(3783,concurrency control in hierarchical multidatabase systems,((co,0),(on,1),(nc,2),(cu,3),(ur,4),(rr,5),(re,6),(en,7)......))
    val docs = getPositionalQGrams(documents, qgramLength)
    val log = LogManager.getRootLogger

    //Sorts the n-grams by their document frequency
    /** From the paper
     * We can extract all the positional q-grams of a string and order them by decreasing order of their idf values and increasing order of their locations.
     */
    //output [(docId,string,[(token index,token position of q-gram),...]),...]
    //q-gram is order by rare decreasing, the rarest one in the head of the array
    val sortedDocs = CommonEdFunctions.getSortedQgrams2(docs)
    sortedDocs.persist(StorageLevel.MEMORY_AND_DISK)
    //log.info("[EDJoin] sorted docs count " + sortedDocs.count()) //low perf
    val ts = Calendar.getInstance().getTimeInMillis
    //output [(tokenId,[strings contain same token])...]
    val prefixIndex = buildPrefixIndex(sortedDocs, qgramLength, threshold)
    prefixIndex.persist(StorageLevel.MEMORY_AND_DISK)
    sortedDocs.unpersist()
    val te = Calendar.getInstance().getTimeInMillis
//    val np = prefixIndex.count()
//    log.info("[EDJoin] Number of elements in the index " + np)
//    if (np>0) {
//      //only use to do the statistics, not a part of the algorithm (n*(n-1)
//      val a = prefixIndex.map(x => x._2.length.toDouble * (x._2.length - 1))
//      val min = a.min()
//      val max = a.max()
//      val cnum = a.sum()
//      val avg = cnum / np
//
//      log.info("[EDJoin] Min number of comparisons " + min)
//      log.info("[EDJoin] Max number of comparisons " + max)
//      log.info("[EDJoin] Avg number of comparisons " + avg)
//      log.info("[EDJoin] Estimated comparisons " + cnum)
//    }
    log.info("[EDJoin] EDJOIN index time (s) " + (te - ts) / 1000.0)

    val t1 = Calendar.getInstance().getTimeInMillis
    val candidates = getCandidatePairs(prefixIndex, qgramLength, threshold)
    prefixIndex.unpersist()
    val t2 = Calendar.getInstance().getTimeInMillis
    //val nc = candidates.count()
    //log.info("[EDJoin] Candidates number " + nc)
    log.info("[EDJoin] EDJOIN join time (s) " + (t2 - t1) / 1000.0)

    candidates
  }

  def getMatches(documents: RDD[(Int, String)], qgramLength: Int, threshold: Int): RDD[(Int, Int, Double)] = {
    val log = LogManager.getRootLogger
    //log.info("[EDJoin] first document " + documents.first())

    val t1 = Calendar.getInstance().getTimeInMillis
    val candidates = getCandidates(documents, qgramLength, threshold)

    val t2 = Calendar.getInstance().getTimeInMillis

    val m = candidates.map { case ((d1Id, d1), (d2Id, d2)) => ((d1Id, d1), (d2Id, d2), CommonEdFunctions.editDist(d1, d2)) }
      .filter(_._3 <= threshold)
      .map { case ((d1Id, d1), (d2Id, d2), ed) => (d1Id, d2Id, ed.toDouble) }
    m.persist(StorageLevel.MEMORY_AND_DISK)
    val t3 = Calendar.getInstance().getTimeInMillis
    //val nm = m.count()
    //log.info("[EDJoin] Num matches " + nm)
    log.info("[EDJoin] Verify time (s) " + (t3 - t2) / 1000.0)
    log.info("[EDJoin] Global time (s) " + (t3 - t1) / 1000.0)
    m
  }


  //  def getMatchesV2(documents: RDD[(Int, Array[String])], qgramLength: Int, threshold: Int): RDD[(Int, Int, Double)] = {
  //    val log = LogManager.getRootLogger
  //    log.info("[EDJoin] first document " + documents.first())
  //
  //    val t1 = Calendar.getInstance().getTimeInMillis
  //    val candidates = getCandidates(documents, qgramLength, threshold)
  //
  //    val t2 = Calendar.getInstance().getTimeInMillis
  //
  //    val m = candidates.map { case ((d1Id, d1), (d2Id, d2)) => ((d1Id, d1), (d2Id, d2), CommonEdFunctions.editDist(d1, d2)) }
  //      .filter(_._3 <= threshold)
  //      .map { case ((d1Id, d1), (d2Id, d2), ed) => (d1Id, d2Id, ed.toDouble) }
  //    m.persist(StorageLevel.MEMORY_AND_DISK)
  //    val nm = m.count()
  //    val t3 = Calendar.getInstance().getTimeInMillis
  //    log.info("[EDJoin] Num matches " + nm)
  //    log.info("[EDJoin] Verify time (s) " + (t3 - t2) / 1000.0)
  //    log.info("[EDJoin] Global time (s) " + (t3 - t1) / 1000.0)
  //    m
  //  }

  def getMatchesWithRate(documents: RDD[(Int, String)], qgramLength: Int, threshold: Int): RDD[(Int, Int, Double)] = {
    val log = LogManager.getRootLogger
    log.info("[EDJoin] first document " + documents.first())

    val t1 = Calendar.getInstance().getTimeInMillis
    val candidates = getCandidates(documents, qgramLength, threshold)

    val t2 = Calendar.getInstance().getTimeInMillis

    val m = candidates.map { case ((d1Id, d1), (d2Id, d2)) => ((d1Id, d1), (d2Id, d2), CommonEdFunctions.editDist(d1, d2) / (d1.length + d2.length)) }
      .filter(_._3 <= threshold)
      .map { case ((d1Id, d1), (d2Id, d2), ed) => (d1Id, d2Id, ed.toDouble) }
    m.persist(StorageLevel.MEMORY_AND_DISK)
    //val nm = m.count()
    //log.info("[EDJoin] Num matches " + nm)
    val t3 = Calendar.getInstance().getTimeInMillis
    log.info("[EDJoin] Verify time (s) " + (t3 - t2) / 1000.0)
    log.info("[EDJoin] Global time (s) " + (t3 - t1) / 1000.0)
    m
  }
}
