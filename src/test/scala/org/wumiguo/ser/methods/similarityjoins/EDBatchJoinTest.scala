package org.wumiguo.ser.methods.similarityjoins

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.similarityjoins.common.ed.CommonEdFunctions
import org.wumiguo.ser.methods.similarityjoins.simjoin.EDBatchJoin._

class EDBatchJoinTest extends AnyFlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)

  it should "getPositionalQGrams with q=2" in {
    val qgram2 = getPositionalQGrams(spark.sparkContext.parallelize(Seq((0, Array("abcd", "efgh")))), 2).collect
    assertResult(
      List(
        (0, 0, "abcd", List(("ab", 0), ("bc", 1), ("cd", 2))),
        (1, 0, "efgh", List(("ef", 0), ("fg", 1), ("gh", 2)))
      )
    )(qgram2.map(x => (x._1, x._2, x._3, x._4.toList)).toList)
  }

  it should "getPositionalQGrams with q=3" in {
    val qgram2 = getPositionalQGrams(spark.sparkContext.parallelize(Seq((0, Array("abcd")))), 3).collect
    assertResult(
      List((0, 0, "abcd", List(("abc", 0), ("bcd", 1))))
    )(qgram2.map(x => (x._1, x._2, x._3, x._4.toList)).toList)
  }

  it should "buildPrefixIndex will filter and group string by token" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (0, "abcd", Array(("ab", 0), ("bc", 1), ("cd", 2))),
        (1, "abbd", Array(("ab", 0), ("bb", 1), ("bd", 2))),
        (2, "abcc", Array(("ab", 0), ("bc", 1), ("cc", 2)))
      ))
    val sortedDocs = CommonEdFunctions.getSortedQgrams2(docs)
    val prefixIndex = buildPrefixIndex(sortedDocs, 2, 1).collect
    assertResult(2)(prefixIndex.size)
    assertResult(
      Array("abcd", "abcc")
    )(prefixIndex(0)._2.map(_._4))
    assertResult(
      List(
        //group by token "bc"
        (4, List((0, 1, List((0, 2), (4, 1), (5, 0)), "abcd"), (2, 1, List((2, 2), (4, 1), (5, 0)), "abcc"))),
        //group by token "ab"
        (5, List((0, 2, List((0, 2), (4, 1), (5, 0)), "abcd"), (1, 2, List((1, 2), (3, 1), (5, 0)), "abbd"), (2, 2, List((2, 2), (4, 1), (5, 0)), "abcc")))
      )
    )(prefixIndex.map(x => (x._1, x._2.map(y => (y._1, y._2, y._3.toList, y._4)).toList)).toList)
    assertResult(
      Array("abcd", "abbd", "abcc")
    )(prefixIndex(1)._2.map(_._4))
  }


  it should "buildPrefixIndexV2 will filter and group string by token" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, 0, "abcd", Array(("ab", 0), ("bc", 1), ("cd", 2))),
        (1, 1, "abbd", Array(("ab", 0), ("bb", 1), ("bd", 2))),
        (1, 2, "abcc", Array(("ab", 0), ("bc", 1), ("cc", 2))),
        (1, 3, "uudd", Array(("uu", 0), ("ud", 1), ("dd", 2)))
      ))
    val sortedDocs = CommonEdFunctions.getSortedQgrams4(docs)
    val prefixIndex = buildPrefixIndexV2(sortedDocs, 2, 1).collect
    assertResult(2)(prefixIndex.size)
    assertResult(
      Array("abcd", "abbd", "abcc")
    )(prefixIndex(0)._2.map(_._4))
    assertResult(
      Array("abcd", "abcc")
    )(prefixIndex(1)._2.map(_._4))
    assertResult(
      List(
        //group by token "bc"
        (8, List((0, 2, List((1, 2), (7, 1), (8, 0)), "abcd"), (1, 2, List((3, 2), (5, 1), (8, 0)), "abbd"), (2, 2, List((4, 2), (7, 1), (8, 0)), "abcc"))),
        //group by token "ab"
        (7, List((0, 1, List((1, 2), (7, 1), (8, 0)), "abcd"), (2, 1, List((4, 2), (7, 1), (8, 0)), "abcc")))
      )
    )(prefixIndex.map(x => (x._1, x._2.map(y => (y._1, y._2, y._3.toList, y._4)).toList)).toList)
  }
  it should "buildPrefixIndexV2 v2 will filter and group string by token" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, 0, "abcde", Array(("ab", 0), ("bc", 1), ("cd", 2), ("de", 3))),
        (1, 1, "abbde", Array(("ab", 0), ("bb", 1), ("bd", 2), ("de", 3))),
        (1, 2, "abcce", Array(("ab", 0), ("bc", 1), ("cc", 2), ("ce", 3)))
      ))
    val sortedDocs = CommonEdFunctions.getSortedQgrams4(docs)
    val prefixIndex = buildPrefixIndexV2(sortedDocs, 2, 1).collect
    assertResult(2)(prefixIndex.size)
    assertResult(
      Array("abcde", "abcce")
    )(prefixIndex(0)._2.map(_._4))
    assertResult(
      Array("abcde", "abbde")
    )(prefixIndex(1)._2.map(_._4))
    assertResult(
      List(
        //group by token "bc"
        (5, List((0, 1, List((0, 2), (5, 1), (6, 3), (7, 0)), "abcde"), (2, 2, List((1, 3), (3, 2), (5, 1), (7, 0)), "abcce"))),
        //group by token "ab"
        (6, List((0, 2, List((0, 2), (5, 1), (6, 3), (7, 0)), "abcde"), (1, 2, List((2, 2), (4, 1), (6, 3), (7, 0)), "abbde"))))
    )(prefixIndex.map(x => (x._1, x._2.map(y => (y._1, y._2, y._3.toList, y._4)).toList)).toList)

    val docs2 = spark.sparkContext.parallelize(
      Seq(
        (1, 0, "abcde", Array(("ab", 0), ("bc", 1), ("cd", 2), ("de", 3))),
        (1, 1, "abbde", Array(("ab", 0), ("bb", 1), ("bd", 2), ("de", 3))),
        (1, 2, "abcce", Array(("ab", 0), ("bc", 1), ("cc", 2), ("ce", 3))),
        (1, 3, "uudd", Array(("uu", 0), ("ud", 1), ("dd", 2)))
      ))
    val sortedDocs2 = CommonEdFunctions.getSortedQgrams4(docs2)
    val prefixIndex2 = buildPrefixIndexV2(sortedDocs2, 2, 1).collect
    assertResult(2)(prefixIndex2.size)
    assertResult(
      Array("abcde", "abcce")
    )(prefixIndex2(0)._2.map(_._4))
    assertResult(
      Array("abcde", "abbde")
    )(prefixIndex2(1)._2.map(_._4))
    assertResult(
      List(
        (8, List((0, 1, List((1, 2), (8, 1), (9, 3), (10, 0)), "abcde"), (2, 2, List((3, 3), (5, 2), (8, 1), (10, 0)), "abcce"))),
        (9, List((0, 2, List((1, 2), (8, 1), (9, 3), (10, 0)), "abcde"), (1, 2, List((4, 2), (6, 1), (9, 3), (10, 0)), "abbde"))))
    )(prefixIndex2.map(x => (x._1, x._2.map(y => (y._1, y._2, y._3.toList, y._4)).toList)).toList)
  }


  it should "buildPrefixIndexV3 will filter and group string by token" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, 0, "abcd", Array(("ab", 0), ("bc", 1), ("cd", 2))),
        (1, 1, "abbd", Array(("ab", 0), ("bb", 1), ("bd", 2))),
        (1, 2, "abcc", Array(("ab", 0), ("bc", 1), ("cc", 2))),
        (1, 3, "uudd", Array(("uu", 0), ("ud", 1), ("dd", 2)))
      ))
    val sortedDocs = CommonEdFunctions.getSortedQgrams4(docs)
    val prefixIndex = buildPrefixIndexV3(sortedDocs, 2, 1).collect
    assertResult(2)(prefixIndex.size)
    assertResult(
      //      Array("abcd", "abbd", "abcc")
      Array("abcd", "abcc")
    )(prefixIndex(0)._3.map(_._4))
    assertResult(
      //      Array("abcd", "abcc")
      Array("abcd", "abbd", "abcc")
    )(prefixIndex(1)._3.map(_._4))
    assertResult(
      List(
        //group by token "ab"
        (7, List((0, 1, List((1, 2), (7, 1), (8, 0)), "abcd"), (2, 1, List((4, 2), (7, 1), (8, 0)), "abcc"))),
        //group by token "bc"
        (8, List((0, 2, List((1, 2), (7, 1), (8, 0)), "abcd"), (1, 2, List((3, 2), (5, 1), (8, 0)), "abbd"), (2, 2, List((4, 2), (7, 1), (8, 0)), "abcc")))
      )
    )(prefixIndex.map(x => (x._2, x._3.map(y => (y._1, y._2, y._3.toList, y._4)).toList)).toList)
  }


  it should "buildPrefixIndex will filter and group string by token v2" in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, String, Array[(String, Int)])](
      (1, "nice day", Array(("nice", 0), (" day", 1))),
      (2, "good day", Array(("good", 0), (" day", 1))),
      (3, "day", Array(("day", 0))),
      (4, "day day up", Array(("day ", 0), ("day ", 1), ("y up", 2)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams2(docsRdd)
    val prefixIndex = buildPrefixIndex(sortedQg, 2, 1).collect.toList
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
    var prefixIndex = buildPrefixIndex(sortedQg, 2, 0).collect.toList
    assertResult(1)(prefixIndex.size)
    assertResult(List(
      Seq("day up", "day day up")
    ))(prefixIndex.map(_._2.map(_._4).toSeq))
    prefixIndex = buildPrefixIndex(sortedQg, 2, 1).collect.toList
    assertResult(4)(prefixIndex.size)
    assertResult(List(
      Seq("date", "test daydate"), //"date"
      Seq("day up", "day day up"), //"y up"
      Seq("day up", "day day up", "day day up"), //"day "
      Seq("nice day", "good day", "goob day", "test daydate") //" day"
    ))(prefixIndex.map(_._2.map(_._4).toSeq))
    prefixIndex = buildPrefixIndex(sortedQg, 2, 2).collect.toList
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
    val prefixIndex = buildPrefixIndex(sortedDocs, 2, 1).collect
    //group by "bc"
    assertResult(
      Array("abcd", "habcd")
    )(prefixIndex(0)._2.map(_._4))
  }

  it should "getMatches simple" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, Array("this")),
        (2, Array("mthis"))
      ))
    val results = getMatches(docs, 3, 0).collect
    assertResult(Array())(results.sortBy(_._1))
  }

  it should "getMatchesV2 should match string within edit distance is 1" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, Array("this string with 1 insert change")),
        (2, Array("mthis string with 1 insert change")),
        (3, Array("this string with 1 substitution change")),
        (4, Array("mhis string with 1 substitution change")),
        (5, Array("this string with 1 delete change")),
        (6, Array("his string with 1 delete change")),
        (7, Array("first")),
        (8, Array("second")),
        (9, Array("same string")),
        (10, Array("same string"))
      ))
    val results1 = getMatchesV2(docs, 3, 1, Map(0 -> 1.0)).collect
    assertResult(
      Array((1, 2, 0.9696969696969697), (3, 4, 0.9736842105263158), (5, 6, 0.96875),(9,10,1.0))
    )(results1.sortBy(_._1))
    val docs2 = spark.sparkContext.parallelize(
      Seq(
        (1, Array("this string with 1 insert change", "test data")),
        (2, Array("mthis string with 1 insert change", "test datu")),
        (3, Array("this string with 1 substitution change", "coin resu")),
        (4, Array("mhis string with 1 substitution change", "join resu")),
        (5, Array("this string with 1 delete change", "noe")),
        (6, Array("his string with 1 delete change", "ne")),
        (7, Array("first")),
        (8, Array("second")),
        (9, Array("same string", "same")),
        (10, Array("same string", "same"))
      ))
    val results2 = getMatchesV2(docs2, 3, 1, Map(0 -> 0.5, 1 -> 0.5)).collect
    assertResult(
      Array((1, 2, 0.9292929292929293), (3, 4, 0.9312865497076024), (5, 6, 0.484375), (9, 10, 1.0))
    )(results2.sortBy(_._1))
    val results3 = getMatchesV2(docs2, 3, 1, Map(0 -> 0.0, 1 -> 1.0)).collect
    assertResult(
      Array((1, 2, 0.8888888888888888), (3, 4, 0.8888888888888888), (5, 6, 0.0), (9, 10, 1.0))
    )(results3.sortBy(_._1))
    val results4 = getMatchesV2(docs2, 2, 1, Map(0 -> 0.0, 1 -> 1.0)).collect
    assertResult(
      Array((1, 2, 0.8888888888888888), (3, 4, 0.8888888888888888), (5, 6, 0.0), (9, 10, 1.0))
    )(results4.sortBy(_._1))
  }


  it should "getMatches exactly aka edit distance=0" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, Array("this string with 1 insert change")),
        (2, Array("mthis string with 1 insert change")),
        (3, Array("this string with 1 substitution change")),
        (4, Array("mhis string with 1 substitution change")),
        (5, Array("this string with 1 delete change")),
        (6, Array("his string with 1 delete change")),
        (7, Array("first")),
        (8, Array("second"))
      ))
    val results = getMatches(docs, 3, 0).collect
    assertResult(Array())(results.sortBy(_._1))
  }


  ignore should "getMatches should match string within edit distance is 1" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, Array("this string with 1 insert change")),
        (2, Array("mthis string with 1 insert change")),
        (3, Array("this string with 1 substitution change")),
        (4, Array("mhis string with 1 substitution change")),
        (5, Array("this string with 1 delete change")),
        (6, Array("his string with 1 delete change")),
        (7, Array("first")),
        (8, Array("second"))
      ))
    val results = getMatches(docs, 3, 1).collect
    assertResult(
      Array((1, 2, 1.0), (3, 4, 1.0), (5, 6, 1.0))
    )(results.sortBy(_._1))
  }

  ignore should "getMatches should not match string within edit distance is 2 when the threadhold is 1" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, Array("this string with 2 insert change")),
        (2, Array("mmthis string with 2 insert change")),
        (3, Array("this string with 2 substitution change")),
        (4, Array("mmis string with 2 substitution change")),
        (5, Array("this string with 2 delete change")),
        (6, Array("is string with 2 delete change"))
      ))
    val results = getMatches(docs, 3, 1).collect
    assertResult(Array())(results)
  }

  it should "getCandidates" in {
    val docs = spark.sparkContext.parallelize(
      Seq(
        (1, Array("this string with 1 insert change")),
        (2, Array("mthis string with 1 insert change")),
        (3, Array("this string with 1 substitution change")),
        (4, Array("mhis string with 1 substitution change")),
        (5, Array("this string with 1 delete change")),
        (6, Array("his string with 1 delete change"))
      ))
    val candis = getCandidatesV0(docs, 3, 1)
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
    val pairRdd = getCandidatePairs(prefixIndex, qgramLength, threshold).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(0)(pairRdd.size)
  }

  ignore should "getCandidatePairs v2" in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, String, Array[(String, Int)])](
      (1, "nice day 001", Array(("nice", 0), (" day", 1), (" 001", 2))),
      (2, "good day 002", Array(("good", 0), (" day", 1), (" 002", 2))),
      (3, "try get cand", Array(("try ", 0), ("get ", 1), ("cand", 2))),
      (4, "try got cand", Array(("try ", 0), ("got ", 1), ("cand", 2))),
      (5, "dry got cand", Array(("dry ", 0), ("got ", 1), ("cand", 2)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams2(docsRdd)
    val prefixIndex = buildPrefixIndex(sortedQg, 2, 1)
    val prefixIndexList = prefixIndex.collect.toList
    assertResult(4)(prefixIndexList.size)
    log.info("prefixIndexList=" + prefixIndexList.map(x => (x._1, x._2.map(y => (y._1, y._2, y._3.toList, y._4)).toList)))
    var qgramLength = 4
    var pairRdd = getCandidatePairs(prefixIndex, qgramLength, 1).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(4)(pairRdd.size)
    pairRdd = getCandidatePairs(prefixIndex, qgramLength, 2).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(4)(pairRdd.size)
    qgramLength = 2
    pairRdd = getCandidatePairs(prefixIndex, qgramLength, 1).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(4)(pairRdd.size)
    pairRdd = getCandidatePairs(prefixIndex, qgramLength, 2).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(4)(pairRdd.size)
  }


  ignore should "getCandidatePairs v3" in {
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
    val prefixIndex = buildPrefixIndex(sortedQg, 2, 1)
    val prefixIndexList = prefixIndex.collect.toList
    assertResult(4)(prefixIndexList.size)
    log.info("prefixIndexList=" + prefixIndexList.map(x => (x._1, x._2.map(y => (y._1, y._2, y._3.toList, y._4)).toList)))
    var qgramLength = 4
    var pairRdd = getCandidatePairs(prefixIndex, qgramLength, 1).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(7)(pairRdd.size)
    pairRdd = getCandidatePairs(prefixIndex, qgramLength, 2).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(8)(pairRdd.size)
    qgramLength = 2
    pairRdd = getCandidatePairs(prefixIndex, qgramLength, 1).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(7)(pairRdd.size)
    pairRdd = getCandidatePairs(prefixIndex, qgramLength, 2).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(8)(pairRdd.size)
  }


  ignore should "getCandidatePairsV2 v3" in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, Int, String, Array[(String, Int)])](
      (0, 1, "nice day", Array(("nice", 0), (" day", 1))),
      (0, 2, "good day", Array(("good", 0), (" day", 1))),
      (0, 3, "date", Array(("date", 0))),
      (0, 4, "goob day", Array(("goob", 0), (" day", 1))),
      (0, 5, "day day up", Array(("day ", 0), ("day ", 1), ("y up", 2))),
      (0, 6, "day up", Array(("day ", 0), ("y up", 1))),
      (0, 7, "test daydate", Array(("test", 0), (" day", 1), ("date", 2)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams4(docsRdd)
    val prefixIndex = buildPrefixIndexV3(sortedQg, 2, 1)
    val prefixIndexList = prefixIndex.collect.toList
    assertResult(4)(prefixIndexList.size)
    log.info("prefixIndexList=" + prefixIndexList.map(x => (x._2, x._3.map(y => (y._1, y._2, y._3.toList, y._4)).toList)))
    var qgramLength = 4
    var pairRdd = getCandidatePairsV2(prefixIndex, qgramLength, 1).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(7)(pairRdd.size)
    pairRdd = getCandidatePairsV2(prefixIndex, qgramLength, 2).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(8)(pairRdd.size)
    qgramLength = 2
    pairRdd = getCandidatePairsV2(prefixIndex, qgramLength, 1).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(7)(pairRdd.size)
    pairRdd = getCandidatePairsV2(prefixIndex, qgramLength, 2).collect
    pairRdd.foreach(x => println("pair=" + x))
    assertResult(8)(pairRdd.size)
  }

}

