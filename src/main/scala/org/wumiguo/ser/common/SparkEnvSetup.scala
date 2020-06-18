package org.wumiguo.ser.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * @author levinliu
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
trait SparkEnvSetup {
  val log = LoggerFactory.getLogger(this.getClass.getName)
  var appConfig = scala.collection.Map[String, Any]()
  var sparkSession: SparkSession = null

  def createSparkSession(applicationName: String, configPath: String = null): SparkSession = {
    try {
      if (sparkSession == null || sparkSession.sparkContext.isStopped) {
        val sparkConf = new SparkConf()
        sparkConf.setAppName(applicationName)
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      }
      sparkSession
    }
    catch {
      case e: Exception => throw new RuntimeException("Fail to initialize spark session", e)
    }
  }
  def createLocalSparkSession(applicationName: String, configPath: String = null): SparkSession = {
    try {
      if (sparkSession == null || sparkSession.sparkContext.isStopped) {
        val sparkConf = new SparkConf()
        sparkConf.setAppName(applicationName)
        sparkConf.setMaster("local[*]")
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      }
      sparkSession
    }
    catch {
      case e: Exception => throw new RuntimeException("Fail to initialize spark session", e)
    }
  }

}
