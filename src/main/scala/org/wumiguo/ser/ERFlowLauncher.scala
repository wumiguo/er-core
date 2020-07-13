package org.wumiguo.ser

import org.slf4j.LoggerFactory
import org.wumiguo.ser.flow.{End2EndSimpleFlow3, SchemaBasedSimJoinECFlow}
import org.wumiguo.ser.methods.util.CommandLineUtil

/**
  * @author levinliu
  *         Created on 2020/6/18
  *         (Change file header on Settings -> Editor -> File and Code Templates)
  */
object ERFlowLauncher {
  val log = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    log.info("start spark-er flow now")

    val flowType = CommandLineUtil.getParameter(args, "flowType", "SSJoin")

    flowType match {
      case "End2End" =>
        //End2EndSimpleFlow2.run(Array[String](""))
        End2EndSimpleFlow3.run(args)
      case "SSJoin" => SchemaBasedSimJoinECFlow.run(args)
    }
    log.info("end spark-er flow now")
  }
}
