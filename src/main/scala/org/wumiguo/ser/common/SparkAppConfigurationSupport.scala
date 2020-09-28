package org.wumiguo.ser.common

import org.wumiguo.ser.flow.configuration.CommandLineConfigLoader
import org.wumiguo.ser.methods.util.CommandLineUtil.getParameter

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/9/24
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object SparkAppConfigurationSupport {

  def sparkConf2Args(sparkConf: SparkAppConfiguration): Array[String] = {
    var args = Array[String]()
    args :+= "spark-master=" + sparkConf.master
    args :+= "spark-enableHiveSupport=" + sparkConf.enableHiveSupport
    args :+= "spark-optionSize=" + sparkConf.options.size
    var i = 0
    for (opt <- sparkConf.options) {
      args :+= "spark-option" + i + "=" + opt._1 + ":" + opt._2
      i += 1
    }
    args
  }

  def args2SparkConf(args: Array[String]): SparkAppConfiguration = {
    val master = getParameter(args, "spark-master", "local[*]")
    val enableHiveSupport = getParameter(args, "spark-enableHiveSupport", "false")
    val size = getParameter(args, "spark-optionSize", "0")
    val options = mutable.Map[String, String]()
    for (i <- 0 until size.toInt) {
      val option = getParameter(args, "spark-option" + i, "")
      if (option.contains(":")) {
        options.put(option.split(":")(0), option.split(":")(1))
      }
    }
    val sparkConf = new SparkAppConfiguration()
    sparkConf.master = master
    sparkConf.enableHiveSupport = enableHiveSupport.toBoolean
    sparkConf.options = options
    sparkConf
  }

}
