package org.wumiguo.ser.dataloader.filter

import org.slf4j.LoggerFactory
import org.wumiguo.ser.methods.datastructure.KeyValue

/**
 * @author levinliu
 *         Created on 2020/8/21
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object SpecificFieldValueFilter extends FieldFilter {
  val log = LoggerFactory.getLogger(this.getClass.getName)

  override def filter(kvList: List[KeyValue], fieldValuesScope: List[KeyValue]): Boolean = {
    if (fieldValuesScope.map(_.key).toSet.size == fieldValuesScope.size) {
      fieldValuesScope.intersect(kvList) == fieldValuesScope
    } else {
      val kvMap = fieldValuesScope.groupBy(_.key).map(x => (x._1, x._2.map(_.value)))
      val matchItem = kvList.filter(x => kvMap.get(x.key).getOrElse(Nil).contains(x.value)).size
      matchItem == kvMap.size
    }
  }
}
