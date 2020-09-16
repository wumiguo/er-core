package org.wumiguo.ser.methods.similarityjoins

import org.apache.spark.rdd.RDD
import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.similarityjoins.common.ed.CommonEdFunctions
import org.wumiguo.ser.methods.similarityjoins.simjoin.EDJoin

class EDJoinTest extends AnyFlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)

  it should "getPositionalQGrams with q=2" in {
    val qgram2 = EDJoin.getPositionalQGrams(spark.sparkContext.parallelize(Seq((0, "abcd"))), 2).collect
    assertResult(
      Array(("ab", 0), ("bc", 1), ("cd", 2))
    )(qgram2(0)._3)
  }

  it should "getPositionalQGrams with q=3" in {
    val qgram3 = EDJoin.getPositionalQGrams(spark.sparkContext.parallelize(Seq((0, "abcd"))), 3).collect
    assertResult(
      Array(("abc", 0), ("bcd", 1))
    )(qgram3(0)._3)
  }

  it should "buildPrefixIndex will filter and group string by token" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (0, "abcd", Array(("ab", 0), ("bc", 1), ("cd", 2))),
        (1, "abbd", Array(("ab", 0), ("bb", 1), ("bd", 2))),
        (2, "abcc", Array(("ab", 0), ("bc", 1), ("cc", 2)))
      ))
    val sortedDocs = CommonEdFunctions.getSortedQgrams2(docs)
    val prefixIndex = EDJoin.buildPrefixIndex(sortedDocs, 2, 1).collect
    assertResult(2)(prefixIndex.size)
    //group by token "bc"
    assertResult(
      Array("abcd", "abcc")
    )(prefixIndex(0)._2.map(_._4))
    //group by token "ab"
    assertResult(
      Array("abcd", "abbd", "abcc")
    )(prefixIndex(1)._2.map(_._4))
  }


  it should "buildPrefixIndex will filter and group string by token v2" in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, String, Array[(String, Int)])](
      (1, "nice day", Array(("nice", 0), (" day", 1))),
      (2, "good day", Array(("good", 0), (" day", 1))),
      (3, "day", Array(("day", 0))),
      (4, "day day up", Array(("day ", 0), ("day ", 1), ("y up", 2)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams2(docsRdd)
    val prefixIndex = EDJoin.buildPrefixIndex(sortedQg, 2, 1).collect.toList
    assertResult(2)(prefixIndex.size)
    //group by token "bc"
    assertResult(
      Array("day day up", "day day up")
    )(prefixIndex(0)._2.map(_._4))
    //group by token "ab"
    assertResult(
      Array("nice day", "good day")
    )(prefixIndex(1)._2.map(_._4))
  }

  it should "buildPrefixIndex will filter and group string by token v3" in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, String, Array[(String, Int)])](
      (1, "nice day", Array(("nice", 0), (" day", 1))),
      (2, "good day", Array(("good", 0), (" day", 1))),
      (3, "date", Array(("date", 0))),
      (4, "goob day", Array(("goob", 0), (" day", 1))),
      (5, "day day up", Array(("day ", 0), ("day ", 1), ("y up", 2))),
      (6, "day up", Array(("day ", 0), ("y up", 1))),
      (7, "test daydate", Array(("test", 0), (" day", 1), ("date", 2)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams2(docsRdd)
    var prefixIndex = EDJoin.buildPrefixIndex(sortedQg, 2, 0).collect.toList
    assertResult(1)(prefixIndex.size)
    assertResult(List(
      Seq("day up", "day day up")
    ))(prefixIndex.map(_._2.map(_._4).toSeq))
    prefixIndex = EDJoin.buildPrefixIndex(sortedQg, 2, 1).collect.toList
    assertResult(4)(prefixIndex.size)
    assertResult(List(
      Seq("date", "test daydate"), //"date"
      Seq("day up", "day day up"), //"y up"
      Seq("day up", "day day up", "day day up"), //"day "
      Seq("nice day", "good day", "goob day", "test daydate") //" day"
    ))(prefixIndex.map(_._2.map(_._4).toSeq))
    prefixIndex = EDJoin.buildPrefixIndex(sortedQg, 2, 2).collect.toList
    assertResult(4)(prefixIndex.size)
    assertResult(List(
      Seq("date", "test daydate"),
      Seq("day up", "day day up"),
      Seq("day up", "day day up", "day day up"),
      Seq("nice day", "good day", "goob day", "test daydate")
    ))(prefixIndex.map(_._2.map(_._4).toSeq))
  }

  it should "buildPrefixIndex will filter value habce since there is 2 change" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (0, "abcd", Array(("ab", 0), ("bc", 1), ("cd", 2))),
        (1, "habce", Array(("ha", 0), ("ab", 1), ("bc", 2), ("ce", 3))),
        (2, "habcd", Array(("ha", 0), ("ab", 1), ("bc", 2), ("cd", 3)))
      ))
    val sortedDocs = CommonEdFunctions.getSortedQgrams2(docs)
    val prefixIndex = EDJoin.buildPrefixIndex(sortedDocs, 2, 1).collect
    //group by "bc"
    assertResult(
      Array("abcd", "habcd")
    )(prefixIndex(0)._2.map(_._4))
  }

  it should "getMatches simple" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, "this"),
        (2, "mthis")
      ))
    val results = EDJoin.getMatches(docs, 3, 0).collect
    assertResult(Array())(results.sortBy(_._1))
  }


  it should "getMatches exactly aka edit distance=0" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, "this string with 1 insert change"),
        (2, "mthis string with 1 insert change"),
        (3, "this string with 1 substitution change"),
        (4, "mhis string with 1 substitution change"),
        (5, "this string with 1 delete change"),
        (6, "his string with 1 delete change"),
        (7, "first"),
        (8, "second")
      ))
    val results = EDJoin.getMatches(docs, 3, 0).collect
    assertResult(Array())(results.sortBy(_._1))
  }


  it should "getMatches should match string within edit distance is 1" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, "this string with 1 insert change"),
        (2, "mthis string with 1 insert change"),
        (3, "this string with 1 substitution change"),
        (4, "mhis string with 1 substitution change"),
        (5, "this string with 1 delete change"),
        (6, "his string with 1 delete change"),
        (7, "first"),
        (8, "second")
      ))
    val results = EDJoin.getMatches(docs, 3, 1).collect
    assertResult(
      Array((1, 2, 1.0), (3, 4, 1.0), (5, 6, 1.0))
    )(results.sortBy(_._1))
  }

  it should "getMatches should not match string within edit distance is 2 when the threadhold is 1" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, "this string with 2 insert change"),
        (2, "mmthis string with 2 insert change"),
        (3, "this string with 2 substitution change"),
        (4, "mmis string with 2 substitution change"),
        (5, "this string with 2 delete change"),
        (6, "is string with 2 delete change")
      ))
    val results = EDJoin.getMatches(docs, 3, 1).collect
    assertResult(Array())(results)
  }

  it should "getCandidates" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, "this string with 1 insert change"),
        (2, "mthis string with 1 insert change"),
        (3, "this string with 1 substitution change"),
        (4, "mhis string with 1 substitution change"),
        (5, "this string with 1 delete change"),
        (6, "his string with 1 delete change")
      ))
    val candis = EDJoin.getCandidates(docs, 3, 1)
    val output = candis.sortBy(_._1._1).collect.toList
    assertResult(3)(output.size)
    assertResult(List(((1, "this string with 1 insert change"), (2, "mthis string with 1 insert change")),
      ((3, "this string with 1 substitution change"), (4, "mhis string with 1 substitution change")),
      ((5, "this string with 1 delete change"), (6, "his string with 1 delete change")))
    )(output)
  }

  it should "getCandidatePairs v1" in {
    val prefixIndex = spark.sparkContext.parallelize(
      Seq[(Int, Array[(Int, Int, Array[(Int, Int)], String)])](
        (1, Array((1, 1, Array((1, 1)), ""))),
        (2, Array((1, 1, Array((1, 1)), "")))
      )
    )
    val qgramLength = 2
    val threshold = 2
    val pairRdd = EDJoin.getCandidatePairs(prefixIndex, qgramLength, threshold).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(0)(pairRdd.size)
  }

  it should "getCandidatePairs v2" in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, String, Array[(String, Int)])](
      (1, "nice day 001", Array(("nice", 0), (" day", 1), (" 001", 2))),
      (2, "good day 002", Array(("good", 0), (" day", 1), (" 002", 2))),
      (3, "try get cand", Array(("try ", 0), ("get ", 1), ("cand", 2))),
      (4, "try got cand", Array(("try ", 0), ("got ", 1), ("cand", 2))),
      (5, "dry got cand", Array(("dry ", 0), ("got ", 1), ("cand", 2)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams2(docsRdd)
    val prefixIndex = EDJoin.buildPrefixIndex(sortedQg, 2, 1)
    val prefixIndexList = prefixIndex.collect.toList
    assertResult(4)(prefixIndexList.size)
    log.info("prefixIndexList=" + prefixIndexList.map(x => (x._1, x._2.map(y => (y._1, y._2, y._3.toList, y._4)).toList)))
    var qgramLength = 4
    var pairRdd = EDJoin.getCandidatePairs(prefixIndex, qgramLength, 1).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(4)(pairRdd.size)
    pairRdd = EDJoin.getCandidatePairs(prefixIndex, qgramLength, 2).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(4)(pairRdd.size)
    qgramLength = 2
    pairRdd = EDJoin.getCandidatePairs(prefixIndex, qgramLength, 1).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(4)(pairRdd.size)
    pairRdd = EDJoin.getCandidatePairs(prefixIndex, qgramLength, 2).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(4)(pairRdd.size)
  }


  it should "getCandidatePairs v3" in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, String, Array[(String, Int)])](
      (1, "nice day", Array(("nice", 0), (" day", 1))),
      (2, "good day", Array(("good", 0), (" day", 1))),
      (3, "date", Array(("date", 0))),
      (4, "goob day", Array(("goob", 0), (" day", 1))),
      (5, "day day up", Array(("day ", 0), ("day ", 1), ("y up", 2))),
      (6, "day up", Array(("day ", 0), ("y up", 1))),
      (7, "test daydate", Array(("test", 0), (" day", 1), ("date", 2)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams2(docsRdd)
    val prefixIndex = EDJoin.buildPrefixIndex(sortedQg, 2, 1)
    val prefixIndexList = prefixIndex.collect.toList
    assertResult(4)(prefixIndexList.size)
    log.info("prefixIndexList=" + prefixIndexList.map(x => (x._1, x._2.map(y => (y._1, y._2, y._3.toList, y._4)).toList)))
    var qgramLength = 4
    var pairRdd = EDJoin.getCandidatePairs(prefixIndex, qgramLength, 1).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(7)(pairRdd.size)
    pairRdd = EDJoin.getCandidatePairs(prefixIndex, qgramLength, 2).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(8)(pairRdd.size)
    qgramLength = 2
    pairRdd = EDJoin.getCandidatePairs(prefixIndex, qgramLength, 1).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(7)(pairRdd.size)
    pairRdd = EDJoin.getCandidatePairs(prefixIndex, qgramLength, 2).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(8)(pairRdd.size)
  }
}

