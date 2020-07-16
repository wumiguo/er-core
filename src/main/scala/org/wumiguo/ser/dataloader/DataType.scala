package org.wumiguo.ser.dataloader

/**
 * @author levinliu
 *         Created on 2020/7/16
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object DataType extends Enumeration {
  type DataType = Value
  val CSV, JSON, PARQUET = Value
}
