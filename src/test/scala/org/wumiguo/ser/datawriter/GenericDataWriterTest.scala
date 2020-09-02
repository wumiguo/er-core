package org.wumiguo.ser.datawriter

import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.dataloader.{CSVProfileLoader, DataTypeResolver, ParquetProfileLoader, ProfileLoaderFactory}
import org.wumiguo.ser.methods.datastructure.{KeyValue}
import org.wumiguo.ser.testutil.TestDirs

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/8/31
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class GenericDataWriterTest extends AnyFlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(this.getClass.getName)

  it should "write parquet " in {
    val overwriteOnExist = true
    val joinResultFile = "res"
    val outputPath = TestDirs.resolveOutputPath("parquet")
    val outputType = "parquet"
    val realIDField = "u_id"
    val columnNames = List(realIDField, "username")
    val rawRows = Seq(
      Row.fromTuple(("LUSLD9921", "lev")),
      Row.fromTuple(("DKJKSE021", "vin")),
      Row.fromTuple(("UEJLS9291", "joh")),
      Row.fromTuple(("YYYEKE021", "ohn"))
    )
    val rows = spark.sparkContext.makeRDD(rawRows)
    val finalOutputPath = GenericDataWriter.generateOutputWithSchema(columnNames, rows,
      outputPath, outputType, joinResultFile, overwriteOnExist)
    log.info("outputPath=" + finalOutputPath)
    assertResult(outputPath + "/res.parquet")(finalOutputPath)
    val loader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(finalOutputPath))
    assert(loader.isInstanceOf[ParquetProfileLoader.type])
    assert(!loader.isInstanceOf[CSVProfileLoader.type])
    val data = loader.load(finalOutputPath, realIDField = realIDField,keepRealID = true)
    val outKv = data.map(p => (p.getAttributeValues(realIDField), p.attributes.toList)).collect().toList
      .map(x => Map(x._1 -> x._2)).reduce(_ ++ _)
    val rawKv = rawRows.map(x => mutable.Map(x.getString(columnNames.indexOf(realIDField)) -> columnNames.map(name => KeyValue(name, x.getString(columnNames.indexOf(name))))))
      .reduce(_ ++ _)
    assertResult(4)(outKv.size)
    assertResult(rawKv)(outKv)
  }
}
