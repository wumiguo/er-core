package org.wumiguo.ser.methods.entitymatching

import org.scalatest.FlatSpec
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}
import org.wumiguo.ser.methods.entitymatching.MatchingFunctions.jaccardSimilarity

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/6/22
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class MatchingFunctionsTest extends FlatSpec {
  it should "get jaccardSimilarity " in {
    var similarity = jaccardSimilarity(KeyValue("title", "testing code"), KeyValue("title", "testing code"))
    println(similarity)
    assertResult(1.0)(similarity)
    similarity = jaccardSimilarity(KeyValue("title", "TESTING code"), KeyValue("title", "testing code"))
    assertResult(1.0)(similarity)
    similarity = jaccardSimilarity(KeyValue("article", "TESTING code"), KeyValue("title", "testing code"))
    assertResult(1.0)(similarity)
    similarity = jaccardSimilarity(KeyValue("title", "code testing"), KeyValue("title", "testing code"))
    assertResult(1.0)(similarity)
    similarity = jaccardSimilarity(KeyValue("title", "write code testing"), KeyValue("title", "testing write code"))
    assertResult(1.0)(similarity)
    similarity = jaccardSimilarity(KeyValue("title", "just do unit-testing"), KeyValue("title", "testing code"))
    println("jaccardSimilarity: " + similarity)
    assert(1.0 > similarity && similarity > 0.1)
    similarity = jaccardSimilarity(KeyValue("title", "just do function verification"), KeyValue("title", "testing code"))
    println("jaccardSimilarity: " + similarity)
    assert(0 == similarity)
  }

  it should " getSimilarityEdges " in {
    val attrs1 = mutable.MutableList[KeyValue](KeyValue("title", "helloworld"), KeyValue("author", "lev"))
    val attrs2 = mutable.MutableList[KeyValue](KeyValue("title", "hello world"), KeyValue("author", "liu"))
    val p1 = Profile(1, attrs1, "jaOkd", 100)
    val p2 = Profile(2, attrs2, "Uja2d", 102)
    val priorityQueue = MatchingFunctions.getSimilarityEdges(p1, p2)
    println("pq is " + priorityQueue)
    assertResult(0)(priorityQueue.size)
    val priorityQueue1 = MatchingFunctions.getSimilarityEdges(p1, p2, 0.1)
    println("pq1 is " + priorityQueue1)
    assertResult(0)(priorityQueue.size)
    val attrs3 = mutable.MutableList[KeyValue](KeyValue("title", "how to write some code"), KeyValue("auth", "lev"))
    val attrs4 = mutable.MutableList[KeyValue](KeyValue("title", "step on write some code"), KeyValue("auth", "lev liu"))
    val p3 = Profile(1, attrs3, "jaOkd", 100)
    val p4 = Profile(2, attrs4, "Uja2d", 102)
    val priorityQueue2 = MatchingFunctions.getSimilarityEdges(p3, p4, 0.8)
    println("pq2 is " + priorityQueue2)
    val priorityQueue3 = MatchingFunctions.getSimilarityEdges(p3, p4)
    println("pq3 is " + priorityQueue3)
    val priorityQueue4 = MatchingFunctions.getSimilarityEdges(p3, p4, 0.3)
    println("pq4 is " + priorityQueue4)
  }
}
