package org.wumiguo.ser.methods.similarityjoins.common.ed

import org.scalatest.FlatSpec

/**
 * @author levinliu
 *         Created on 2020/7/17
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class EdFiltersTest extends FlatSpec {
  it should "getPrefixLen " in {
    val len = EdFilters.getPrefixLen(2, 2)
    assertResult(2 * 2 + 1)(len)
    val len2 = EdFilters.getPrefixLen(3, 2)
    assertResult(3 * 2 + 1)(len2)
  }

}
