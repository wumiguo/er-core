package org.wumiguo.ser.methods.similarityjoins

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.similarityjoins.datastructure.Category
import org.wumiguo.ser.methods.similarityjoins.simjoin.PartEnum
import org.wumiguo.ser.methods.similarityjoins.simjoin.PartEnum.{getCategories, tokenize}

import scala.collection.mutable.ArrayBuffer

class PartEnumTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)

  it should "tokenize will separate the string by character not number and alphabet" in {
    val tokens = PartEnum.tokenize(
      spark.sparkContext.parallelize(Seq(
        (0, "this is a test with white space something"),
        (1, "this is a test with quote \" something"),
        (2, "this is a test with comma , something")))
    )

    tokens.collect.sortBy(_._1).map(_._3).map(_.mkString(",")).foreach(println(_))
    assertResult(
      Array(
        Array("this", "is", "a", "test", "with", "white", "space", "something"),
        Array("this", "is", "a", "test", "with", "quote", "something"),
        Array("this", "is", "a", "test", "with", "comma", "something")
      )
    )(tokens.collect.sortBy(_._1).map(_._3))
  }

  it should "getCategories with threshold 0.9" in {
    val categories = PartEnum.getCategories(0.9, 50)
    assertResult(
      ArrayBuffer(
        (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 10),
        (11, 12), (13, 14), (15, 16), (17, 18), (19, 21), (22, 24), (25, 27),
        (28, 31), (32, 35), (36, 40), (41, 45), (46, 51))
    )(categories.sortBy(_.s_len).map(t => (t.s_len, t.e_len)))
  }

  it should "getCategories with threshold 0.5" in {
    val categories = PartEnum.getCategories(0.5, 50)
    assertResult(
      ArrayBuffer((1, 2), (3, 6), (7, 14), (15, 30), (31, 62))
    )(categories.sortBy(_.s_len).map(t => (t.s_len, t.e_len)))
  }

  it should "sizeBasedFiltering will group the candidate by their token length" in {
    val documents = spark.sparkContext.parallelize(Seq(
      (0, "3 length candidate"),
      (1, "4 length candidate 1"),
      (2, "7 length candidate 1 2 3 4"),
      (3, "8 length candidate 1 2 3 4 5"),
      (4, "10 length candidate 1 2 3 4 5 6 7"),
      (5, "12 length candidate 1 2 3 4 5 6 7 8 9"),
      (6, "20 length candidate 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17"),
      (7, "another 20 length candidate 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16")
    ))

    val tokens = tokenize(documents)
    val categories: ArrayBuffer[Category] = getCategories(0.9, 50)

    val groupedCandidates = PartEnum.sizeBasedFiltering(tokens, categories)

    assertResult(
      Array(
        (2, 0, "3 length candidate"), (2, 1, "4 length candidate 1"),
        (3, 1, "4 length candidate 1"), (6, 2, "7 length candidate 1 2 3 4"),
        (6, 3, "8 length candidate 1 2 3 4 5"), (7, 3, "8 length candidate 1 2 3 4 5"),
        (7, 4, "10 length candidate 1 2 3 4 5 6 7"), (8, 4, "10 length candidate 1 2 3 4 5 6 7"),
        (8, 5, "12 length candidate 1 2 3 4 5 6 7 8 9"), (9, 5, "12 length candidate 1 2 3 4 5 6 7 8 9"),
        (13, 6, "20 length candidate 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17"), (13, 7, "another 20 length candidate 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16"))
    )(groupedCandidates.
      sortBy(_._1).
      map(_._2).
      collect.flatten.
      map(t => (t._1, t._2, t._3)).sortBy(_._2)
    )
  }

}
