package org.wumiguo.ser.testutil

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.methods.datastructure.Profile

/**
 * @author levinliu
 *         Created on 2020/01/01
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object CollectionsComparision extends AnyFlatSpec {


  def assertSameListIgnoreOrder(expect: List[List[String]], actual: List[List[String]]) = {
    assert(sameIgnoreOrder(expect, actual))
  }

  def assertSameIgnoreOrder(expect: List[Any], actual: List[Any]) = {
    assert(sameIgnoreOrder(expect, actual))
  }


  def sameIgnoreOrder(expect: List[Any], actual: List[Any]): Boolean = {
    if (expect.size != actual.size) {
      assertResult(expect)(actual)
      false
    } else {
      var counter = 0
      for (p1 <- expect) {
        for (p2 <- actual) {
          if (p1 == p2) {
            counter = counter + 1
          }
        }
      }
      val result = counter == expect.size
      if (!result) {
        assertResult(expect)(actual)
      }
      result
    }
  }
}
