package org.wumiguo.ser.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * @author levinliu
 *         Created on 2020/11/8
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
trait SparkTestingEnvSetup {

  val log = LoggerFactory.getLogger(this.getClass.getName)
  var appConfig = scala.collection.Map[String, Any]()
  var sparkSession: SparkSession = null

  val spark = createLocalTestingSparkSession(this.getClass.getName)

  def createLocalTestingSparkSession(applicationName: String, configPath: String = null, outputDir: String = "/tmp/er"): SparkSession = {
    try {
      if (sparkSession == null || sparkSession.sparkContext.isStopped) {
        val sparkConf = new SparkConf()
        sparkConf.setAppName(applicationName)
          .setMaster("local[*]")
          .set("spark.default.parallelism", "4")
          .set("spark.local.dir", outputDir)
        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      }
      sparkSession
    }
    catch {
      case e: Exception => throw new RuntimeException("Fail to initialize spark session", e)
    }
  }

}
