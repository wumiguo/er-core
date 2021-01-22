package org.wumiguo.ser.datawriter

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * GenericDataWriter to write data as give type
 */
/**
 * @author levinliu
 *         Created on 2020/8/29
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object GenericDataWriter {
  val log = LoggerFactory.getLogger(this.getClass.getName)

  def generateOutputWithSchema(columnNames: Seq[String], rows: RDD[Row],
                               outputPath: String, outputType: String, fileName: String = "",
                               overwrite: Boolean = false) = {
    import org.apache.spark.sql.types._
    var fields = mutable.MutableList[StructField]()
    columnNames.map(cn => fields :+= StructField(cn, StringType, nullable = true))
    val schema = StructType(fields.toList)
    generateOutputAdvanced(schema, rows, outputPath,
      outputType, fileName, overwrite, true)
  }

  def generateOutputAdvanced(schema: StructType, rows: RDD[Row],
                             outputPath: String, outputType: String, fileName: String = "",
                             overwrite: Boolean = false,
                             showHeader: Boolean = false): String = {
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.createDataFrame(rows, schema)
    val size = df.rdd.partitions.size
    log.info("default output partitionSize=" + size)
    val outputPartSize = spark.sparkContext.getConf.get("outputPartitionSize", null)
    val finalDf = if (outputPartSize == null) {
      df
    } else {
      log.info("change output partitionSize to " + outputPartSize)
      df.repartition(outputPartSize.toInt)
    }
    val finalPath = if (fileName == null || fileName == "") {
      val inputFormat = new SimpleDateFormat("yyyy-MM-dd_HHmmss")
      outputPath + "/" + inputFormat.format(new Date()) + "." + outputType
    } else {
      outputPath + "/" + fileName + "." + outputType
    }
    val writer = finalDf.write
    if (showHeader) {
      writer.option("header", true)
    }
    if (overwrite) {
      writer.mode(SaveMode.Overwrite)
    }
    outputType.toLowerCase match {
      case "csv" => writer.csv(finalPath)
      case "json" => writer.json(finalPath)
      case "parquet" => writer.parquet(finalPath)
      case "parq" => writer.parquet(finalPath)
      case _ => throw new RuntimeException("Given type [" + outputType.toLowerCase + "] out of support output type")
    }
    finalPath
  }

}
