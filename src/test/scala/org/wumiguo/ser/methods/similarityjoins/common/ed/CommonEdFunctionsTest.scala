package org.wumiguo.ser.methods.similarityjoins.common.ed

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup

/**
 * @author levinliu
 *         Created on 2020/7/17
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class CommonEdFunctionsTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "getQgramsTf v1 with abnormal input" in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, Array[(String, Int)])](
      (1, Array(("hello", 2), ("bd", 1), ("spark", 4), ("program", 0))),
      (2, Array(("world", 5), ("tech", 3), ("test", 2))),
      (3, Array(("hello", 3))),
      (4, Array()),
      (5, Array(("spark", 4), ("bd", 1), ("bd", 6)))
    ))
    val map = CommonEdFunctions.getQgramsTf(docsRdd)
    map.foreach(x => println(x))
    assertResult(3)(map.get("bd").get)
    assertResult(1)(map.get("program").get)
  }

  it should "getQgramsTf v2 with normal input with equal string size in doc array" in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, Array[(String, Int)])](
      (1, Array(("hell", 0), ("o bi", 1), ("gdat", 2), ("a tec", 3), ("tech", 4))), //"hello bigdata tech"
      (2, Array(("it's", 0), (" all", 1), (" abo", 2), ("ut t", 3), ("tech", 4))), //"it's all about tech"
      (3, Array(("samp", 0), ("le d", 1), ("data", 2))), //"sample data",
      (4, Array(("hi", 0))) //"hi"
    ))
    val mapRdd = CommonEdFunctions.getQgramsTf(docsRdd)
    mapRdd.foreach(x => println("qgram=" + x))
    assertResult(None)(mapRdd.get("bd"))
    assertResult(1)(mapRdd.get("it's").get)
    assertResult(2)(mapRdd.get("tech").get)
    assertResult(1)(mapRdd.get("hi").get)
  }

  it should "getQgrams" in {
    val str = "hello bigdata-tech, handle super big volume of data in the world! bigdata is a great tech in the world"
    val qgramSize = 6
    val result = CommonEdFunctions.getQgrams(str, qgramSize)
    result.foreach(x => println("gram=" + x))
    assertResult(("hello ", 0))(result.head)
    assertResult((" world", result.size - 1))(result.last)
  }

  it should "getSortedQgrams2 v1 " in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, String, Array[(String, Int)])](
      (1, "nice day", Array(("nice day", 0))),
      (2, "good day", Array(("good day", 0))),
      (3, "day", Array(("day", 0))),
      (4, "day day up", Array(("day day ", 0), ("y day up", 1)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams2(docsRdd)
    val data = sortedQg.collect.toList
    //0="nice day", 1="good day", 2="day day", 3="day", 4="y day up",
    assertResult(List(
      (1, "nice day", List((0, 0))),
      (2, "good day", List((1, 0))),
      (3, "day", List((3, 0))),
      (4, "day day up", List((2, 1), (4, 0)))
    ))(data.map(x => (x._1, x._2, x._3.toList)))
  }

  it should "getSortedQgrams2 v2 " in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, String, Array[(String, Int)])](
      (1, "nice day", Array(("nice", 0), (" day", 1))),
      (2, "good day", Array(("good", 0), (" day", 1))),
      (3, "day", Array(("day", 0))),
      (4, "day day up", Array(("day ", 0), ("day ", 1), ("y up", 2)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams2(docsRdd)
    val data = sortedQg.collect.toList
    //0=good, 1=nice, 2=y up, 3=day,
    //4="day ", 5=" day"
    assertResult(List(
      (1, "nice day", List((1, 0), (5, 1))),
      (2, "good day", List((0, 0), (5, 1))),
      (3, "day", List((3, 0))),
      (4, "day day up", List((2, 2), (4, 0), (4, 1)))
    ))(data.map(x => (x._1, x._2, x._3.toList)))
  }

  it should "getSortedQgrams2 v3 " in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, String, Array[(String, Int)])](
      (1, "hello bigdata tech", Array(("hell", 0), ("o bi", 1), ("gdat", 2), ("a tec", 3), ("tech", 4))),
      (2, "it's all about tech", Array(("it's", 0), (" all", 1), (" abo", 2), ("ut t", 3), ("tech", 4))),
      (3, "sample data", Array(("samp", 0), ("le d", 1), ("data", 2))),
      (4, "hi", Array(("hi", 0))),
      (6, "all data", Array(("all ", 0), ("data", 1))),
      (7, "tech", Array(("tech", 0))),
      (8, "uuou", Array(("uuou", 0)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams2(docsRdd)
    sortedQg.foreach(x => println("sorted=" + x._1 + "," + x._2 + "," + x._3.toList))
  }

  it should "getSortedQgrams2 v4 " in {
    val docsRdd = spark.sparkContext.makeRDD(Seq[(Int, String, Array[(String, Int)])](
      (1, "hellobigdatatech", Array(("hell", 0), ("obig", 1), ("data", 2), ("tech", 3))),
      (2, "itisallabouttech", Array(("itis", 0), ("alla", 1), ("bout", 2), ("tech", 4))),
      (3, "sampledata", Array(("samp", 0), ("leda", 1), ("data", 2))),
      (4, "hi data", Array(("hi d", 0), ("data", 1))),
      (6, "alldata", Array(("alld", 0), ("data", 1))),
      (7, "tech", Array(("tech", 0))),
      (8, "helloall", Array(("hell", 0), ("oall", 1)))
    ))
    val sortedQg = CommonEdFunctions.getSortedQgrams2(docsRdd)
    val data = sortedQg.collect.toList
    data.foreach(x => println("sorted=" + x._1 + "," + x._2 + "," + x._3.toList))
    //0=itis, 1=samp, 2=obig, 3=0all,
    //4=leda, 5=alla, 6=bout, 7=alld
    //8=hi a, 9=hell, 10=tech, 11=data
    assertResult(List(
      (1, "hellobigdatatech", List((2, 1), (9, 0), (10, 3), (11, 2))),
      (2, "itisallabouttech", List((0, 2), (5, 0), (6, 1), (10, 4))),
      (3, "sampledata", List((1, 0), (4, 1), (11, 2))),
      (4, "hi data", List((8, 0), (11, 1))),
      (6, "alldata", List((7, 0), (11, 1))),
      (7, "tech", List((10, 0))),
      (8, "helloall", List((3, 1), (9, 0)))
    ))(data.map(x => (x._1, x._2, x._3.toList)))
  }

  it should "get editDist" in {
    var dist = CommonEdFunctions.editDist(Seq("abc", "cde"), Seq("abc", "cde"))
    assertResult(0)(dist)
    dist = CommonEdFunctions.editDist(Seq("abc", "cde"), Seq("abc", "cdd"))
    assertResult(1)(dist)
    dist = CommonEdFunctions.editDist(Seq("abc", "cde"), Seq("cde", "abc"))
    assertResult(2)(dist)
    dist = CommonEdFunctions.editDist(Seq("abc", "cde"), Seq("bcc", "cdf"))
    assertResult(2)(dist)
    dist = CommonEdFunctions.editDist(Seq("abc", "cde"), Seq("buc", "cdf"))
    assertResult(2)(dist)
    dist = CommonEdFunctions.editDist(Seq("abc", "cde", "efg"), Seq("buc", "cdf"))
    assertResult(3)(dist)
    dist = CommonEdFunctions.editDist(Seq("abc", "cde", "buc"), Seq("buc"))
    assertResult(2)(dist)
    dist = CommonEdFunctions.editDist(Seq("abc", "cde", "buc"), Seq("buc", "cdf"))
    assertResult(3)(dist)
    dist = CommonEdFunctions.editDist(Seq("abcd", "cde", "buc"), Seq("buc", "cdf"))
    assertResult(3)(dist)
    dist = CommonEdFunctions.editDist(Seq("abcd", "cde", "buc"), Seq("buc", "cdf", "gge"))
    assertResult(3)(dist)
  }
}
