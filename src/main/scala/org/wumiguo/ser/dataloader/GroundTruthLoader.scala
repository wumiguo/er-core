package org.wumiguo.ser.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.wumiguo.ser.methods.datastructure.MatchingEntities

/**
 * @author levinliu
 *         Created on 2020/9/1
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object GroundTruthLoader {

  def loadGroundTruth(filePath: String): RDD[MatchingEntities] = {
    loadGroundTruth(filePath, ",", false)
  }

  def loadGroundTruth(filePath: String, separator: String = ",", header: Boolean = false): RDD[MatchingEntities] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.option("header", header).option("sep", separator).csv(filePath)
    df.rdd map {
      row =>
        MatchingEntities(row.get(0).toString, row.get(1).toString)
    }
  }
}
