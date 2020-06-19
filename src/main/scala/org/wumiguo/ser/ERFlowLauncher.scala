package org.wumiguo.ser

import org.wumiguo.ser.flow.End2EndFlow

/**
 * @author levinliu
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object ERFlowLauncher{
  def main(args: Array[String]): Unit = {
     End2EndFlow.run
  }
}
