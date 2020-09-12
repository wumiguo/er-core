package org.wumiguo.ser.flow.configuration

import org.wumiguo.ser.entity.parameter.DataSetConfig
import org.wumiguo.ser.methods.datastructure.KeyValue

import scala.beans.BeanProperty
import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/9/2
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
case class DataSetConfiguration(
                                 @BeanProperty var path: String,
                                 @BeanProperty var format: String,
                                 @BeanProperty var idField: String,
                                 @BeanProperty var joinAttrs: Array[String] = Array(),
                                 @BeanProperty var additionalAttrs: Array[String] = Array(),
                                 @BeanProperty var filterOptions: Array[KeyValue] = Array()
                               ) {
  override def toString: String = s"DataSetConfiguration(path: $path," +
    s" idField: $idField," +
    s" format: $format," +
    s" joinAttrs: ${joinAttrs.toList}," +
    s" filterOptions: ${filterOptions.toList}," +
    s" additionalAttrs: ${additionalAttrs.toList}" +
    s")"

  def includeRealID(): Boolean = idField != null && !idField.trim.isEmpty && joinAttrs.contains(idField)

}
