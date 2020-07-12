package org.wumiguo.ser.flow

/**
 * @author levinliu
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
trait ERFlow extends Serializable {
  def run(args: Array[String]): Unit
}
