package org.wumiguo.ser

import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.flow.SchemaBasedSimJoinECFlow.{createLocalSparkSession}
import org.wumiguo.ser.flow.configuration.FlowOptions

import scala.reflect.io.File

/**
 * @author levinliu
 *         Created on 2020/7/17
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object CallERFlowLauncherLocally extends SparkEnvSetup {
  def main(args: Array[String]): Unit = {
    val outputDir: File = File("/tmp/data-er")
    if (!outputDir.exists) {
      outputDir.createDirectory(true)
    }
    val spark = createLocalSparkSession(getClass.getName, outputDir = outputDir.path)
//    CallERFlowLauncher.main(args)
    CallERFlowNoIdLauncher.main(args)
  }
}
