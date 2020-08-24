package org.wumiguo.ser.dataloader.filter

import org.wumiguo.ser.methods.datastructure.KeyValue

/**
 * @author levinliu
 *         Created on 2020/8/21
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
trait FieldFilter extends Serializable {
  def filter(kvList: List[KeyValue], fieldValuesScope: List[KeyValue]): Boolean;
}
