package org.wumiguo.ser.flow.configuration

import org.wumiguo.ser.methods.datastructure.KeyValue

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/8/21
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object FilterOptions {

  def getOptions(dataSetPrefix: String, args: Array[String]): List[KeyValue] = {
    val kvList = mutable.MutableList[KeyValue]()
    val optionSizeStr = args.filter(_.startsWith(dataSetPrefix + "-filterSize=")).head
    val size = optionSizeStr.split("=")(1).toInt
    for (i <- (0 to size - 1)) {
      val optionStr = args.filter(_.startsWith(dataSetPrefix + "-filter" + i + "=")).head
      val kv = optionStr.split("=")(1).split(":")
      if (kv(1).contains(",")) {
        val values = kv(1).split(",")
        kvList ++= values.map(KeyValue(kv(0), _))
      } else {
        kvList += KeyValue(kv(0), kv(1))
      }
    }
    kvList.toList
  }


}
