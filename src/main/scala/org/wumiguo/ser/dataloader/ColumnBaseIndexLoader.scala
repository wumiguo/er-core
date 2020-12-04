package org.wumiguo.ser.dataloader

import org.apache.spark.sql.SparkSession
import org.wumiguo.ser.methods.util.PrintContext

/**
 * @author levinliu
 *         Created on 2020/12/3
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object ColumnBaseIndexLoader {

  def loadIndexMap(filePath: String): Map[Int, String] = {
    val dataType = DataTypeResolver.getDataType(filePath)
    dataType match {
      case DataType.CSV => loadCsvHeaderIndexMap(filePath)
      case DataType.PARQUET => loadParquetColumnIndexMap(filePath)
      case _ => throw new RuntimeException("Do not support type " + dataType)
    }
  }

  private def loadCsvHeaderIndexMap(filePath: String, separator: String = ",", header: Boolean = true): Map[Int, String] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    PrintContext.printSession(sparkSession)
    val df = sparkSession.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)
    val columnNames = df.columns
    columnNames.zipWithIndex.map(x => x._2 -> x._1).toMap
  }

  private def loadParquetColumnIndexMap(filePath: String) = {
    val sparkSession = SparkSession.builder().getOrCreate()
    PrintContext.printSession(sparkSession)
    val df = sparkSession.read.parquet(filePath)
    val columnNames = df.schema.fields.map(_.name)
    columnNames.zipWithIndex.map(x => x._2 -> x._1).toMap
  }
}
