package org.wumiguo.ser.dataloader.filter

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.methods.datastructure.KeyValue

/**
 * @author levinliu
 *         Created on 2020/8/20
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class DummyFieldValueFilterTest extends AnyFlatSpec {
  it should "always filter in" in {
    val kvList = List(
      KeyValue("name", "test"), KeyValue("title", "a dummy filter")
    )
    val fieldValuesScope = List(
      KeyValue("name", "____")
    )
    assert(DummyFieldFilter.filter(kvList, fieldValuesScope))
  }
}
