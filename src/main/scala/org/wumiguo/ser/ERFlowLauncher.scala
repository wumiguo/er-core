package org.wumiguo.ser

import org.slf4j.LoggerFactory
import org.wumiguo.ser.flow.{ERFlow, End2EndSimpleFlow, End2EndSimpleFlowSample, SchemaBasedBatchSimJoinECFlow, SchemaBasedBatchV2SimJoinECFlow, SchemaBasedBatchV2SimJoinECPreloadFlow, SchemaBasedSimJoinECFlow, SchemaBasedSimJoinECFlowSample, SchemaBasedSimJoinECParallelFlow, SchemaBasedSimJoinECPreloadFlow}
import org.wumiguo.ser.methods.util.CommandLineUtil
import org.wumiguo.ser.methods.util.CommandLineUtil.getParameter

/**
 * @author levinliu
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object ERFlowLauncher {
  val log = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    log.info("start spark-er flow now")
    val flowType = getParameter(args, "flowType", "SSJoin")
    val flow: ERFlow = flowType match {
      case "End2End" => End2EndSimpleFlow
      case "End2EndSample" => End2EndSimpleFlowSample
      case "SSJoinSample" => SchemaBasedSimJoinECFlowSample
      case "SSJoinPosL" => SchemaBasedSimJoinECFlow // post loading on additional attributes
      case "SSJoin" => SchemaBasedSimJoinECPreloadFlow
      case "SSParaJoin" => SchemaBasedSimJoinECParallelFlow
      case "SSBatchJoin" => SchemaBasedBatchSimJoinECFlow
      case "SSBatchV2Join" => SchemaBasedBatchV2SimJoinECPreloadFlow
      case "SSBatchV2JoinPosL" => SchemaBasedBatchV2SimJoinECFlow
      case _ => throw new RuntimeException("Unsupported flow type " + flowType)
    }
    flow.run(args)
    log.info("end spark-er flow now")
  }
}
