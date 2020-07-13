package org.wumiguo.ser

import org.slf4j.LoggerFactory
import org.wumiguo.ser.flow.{End2EndFlow, End2EndSimpleFlow, End2EndSimpleFlow2, End2EndSimpleFlow3}
import org.wumiguo.ser.flow.End2EndFlow.{createLocalSparkSession, getClass}

/**
 * @author levinliu
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object ERFlowLauncher {
  val log = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    log.info("start spark-er flow now")
    // End2EndFlow.run
    End2EndSimpleFlow3.run(Array[String](""))
    //End2EndSimpleFlow2.run(Array[String](""))
    log.info("end spark-er flow now")
  }
}
