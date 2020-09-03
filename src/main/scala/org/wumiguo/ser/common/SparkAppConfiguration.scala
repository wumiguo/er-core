package org.wumiguo.ser.common

import scala.beans.BeanProperty
import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/9/2
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class SparkAppConfiguration {
  @BeanProperty var master: String = ""
  @BeanProperty var enableHiveSupport: Boolean = false
  @BeanProperty var options: mutable.Map[String, String] = mutable.Map()

}
