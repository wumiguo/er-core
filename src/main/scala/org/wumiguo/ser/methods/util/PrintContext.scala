package org.wumiguo.ser.methods.util

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * @author levinliu
 *         Created on 2020/9/2
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object PrintContext {
  val log = LoggerFactory.getLogger(this.getClass.getName)

  def printSparkContext() = {
    printSession(SparkSession.builder().getOrCreate())
  }

  def printSession(spark: SparkSession) = {
    log.info("-pc-sparkContext master=" + spark.sparkContext.master)
    log.info("-pc-sparkContext user=" + spark.sparkContext.sparkUser)
    log.info("-pc-sparkContext startTime=" + spark.sparkContext.startTime)
    log.info("-pc-sparkContext appName=" + spark.sparkContext.appName)
    log.info("-pc-sparkContext applicationId=" + spark.sparkContext.applicationId)
    log.info("-pc-sparkContext getConf=" + spark.sparkContext.getConf)
    log.info("-pc-sparkContext allConf=" + spark.sparkContext.getConf.getAll.toList)
  }

}
