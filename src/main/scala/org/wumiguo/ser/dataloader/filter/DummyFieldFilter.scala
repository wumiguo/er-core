package org.wumiguo.ser.dataloader.filter

import org.wumiguo.ser.methods.datastructure.KeyValue

/**
 * @author levinliu
 *         Created on 2020/8/23
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object DummyFieldFilter extends FieldFilter {
  override def filter(kvList: List[KeyValue], fieldValuesScope: List[KeyValue]): Boolean = true
}
