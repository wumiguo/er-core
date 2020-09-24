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

  def conf2Args(): Array[String] = {
    var args = Array[String]()
    args :+= "spark-master=" + master
    args :+= "spark-enableHiveSupport=" + enableHiveSupport
    args :+= "spark-optionSize=" + options.size
    var i = 0
    for (opt <- options) {
      args :+= "spark-option" + i + "=" + opt._1 + ":" + opt._2
      i += 1
    }
    args
  }


  override def toString: String = s"SparkAppConfiguration(master: $master, enableHiveSupport: $enableHiveSupport," +
    s" options: $options)"

}
