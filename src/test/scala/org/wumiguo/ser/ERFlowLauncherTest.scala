package org.wumiguo.ser

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.dataloader.{DataTypeResolver, ProfileLoaderFactory}
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}
import org.wumiguo.ser.testutil.TestDirs

import scala.collection.mutable
import scala.reflect.io.File

/**
 * @author levinliu
 *         Created on 2020/9/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class ERFlowLauncherTest extends AnyFlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)

  it should "call ERFlowLauncher v1" in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSJoin"
    flowArgs :+= "dataSet1=" + TestDirs.resolveDataPath("flowdata/dt01.csv")
    flowArgs :+= "dataSet1-id=" + "t_id"
    flowArgs :+= "dataSet1-format=" + "csv"
    flowArgs :+= "dataSet1-attrSet=" + "t_pid"
    flowArgs :+= "dataSet1-filterSize=2"
    flowArgs :+= "dataSet1-filter0=site:CN"
    flowArgs :+= "dataSet1-filter1=t_date:20200715"
    flowArgs :+= "dataSet1-additionalAttrSet=t_pid"
    flowArgs :+= "dataSet2=" + TestDirs.resolveDataPath("flowdata/dp01.csv")
    flowArgs :+= "dataSet2-id=" + "p_id"
    flowArgs :+= "dataSet2-format=" + "csv"
    flowArgs :+= "dataSet2-attrSet=" + "p_id"
    flowArgs :+= "dataSet2-filterSize=1"
    flowArgs :+= "dataSet2-filter0=type:fund"
    flowArgs :+= "dataSet2-additionalAttrSet="
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs :+= "q=2"
    flowArgs :+= "optionSize=3"
    flowArgs :+= "option0=q:2"
    flowArgs :+= "option1=threshold:1" //0,1,2
    flowArgs :+= "option2=algorithm:EDJoin"
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_join"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_join.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assertResult(List(
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"),
        KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1001")), "", 0),
      Profile(1, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"),
        KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1006")), "", 0))
    )(items)
  }

  it should "call ERFlowLauncher v2" in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSJoin"
    flowArgs :+= "dataSet1=" + TestDirs.resolveDataPath("flowdata/dt01.csv")
    flowArgs :+= "dataSet1-id=" + "t_id"
    flowArgs :+= "dataSet1-format=" + "csv"
    flowArgs :+= "dataSet1-attrSet=" + "t_pid"
    flowArgs :+= "dataSet1-filterSize=2"
    flowArgs :+= "dataSet1-filter0=site:CN"
    flowArgs :+= "dataSet1-filter1=t_date:20200715"
    flowArgs :+= "dataSet1-additionalAttrSet=t_pid,system_id"
    flowArgs :+= "dataSet2=" + TestDirs.resolveDataPath("flowdata/dp01.csv")
    flowArgs :+= "dataSet2-id=" + "p_id"
    flowArgs :+= "dataSet2-format=" + "csv"
    flowArgs :+= "dataSet2-attrSet=" + "p_id"
    flowArgs :+= "dataSet2-filterSize=1"
    flowArgs :+= "dataSet2-filter0=type:fund"
    flowArgs :+= "dataSet2-additionalAttrSet=p_name"
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs :+= "q=2"
    flowArgs :+= "optionSize=3"
    flowArgs :+= "option0=q:2"
    flowArgs :+= "option1=threshold:1" //0,1,2
    flowArgs :+= "option2=algorithm:EDJoin"
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_join2"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_join2.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assertResult(
      List(
        Profile(0, mutable.MutableList(
          KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"),
          KeyValue("P1-system_id", "TENCGG"), KeyValue("P2-ID", "PU1001"), KeyValue("P2-p_name", "FinTechETF")), "", 0),
        Profile(1, mutable.MutableList(
          KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"),
          KeyValue("P1-system_id", "TENCGG"), KeyValue("P2-ID", "PU1006"), KeyValue("P2-p_name", "FunStock")), "", 0)
      )
    )(items)
  }

  it should "call ERFlowLauncher para-join" in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSParaJoin"
    flowArgs :+= "dataSet1=" + TestDirs.resolveDataPath("flowdata/dt01.csv")
    flowArgs :+= "dataSet1-id=" + "t_id"
    flowArgs :+= "dataSet1-format=" + "csv"
    flowArgs :+= "dataSet1-attrSet=" + "t_pid"
    flowArgs :+= "dataSet1-filterSize=2"
    flowArgs :+= "dataSet1-filter0=site:CN"
    flowArgs :+= "dataSet1-filter1=t_date:20200715"
    flowArgs :+= "dataSet1-additionalAttrSet=t_pid"
    flowArgs :+= "dataSet2=" + TestDirs.resolveDataPath("flowdata/dp01.csv")
    flowArgs :+= "dataSet2-id=" + "p_id"
    flowArgs :+= "dataSet2-format=" + "csv"
    flowArgs :+= "dataSet2-attrSet=" + "p_id"
    flowArgs :+= "dataSet2-filterSize=1"
    flowArgs :+= "dataSet2-filter0=type:fund"
    flowArgs :+= "dataSet2-additionalAttrSet="
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs :+= "q=2"
    flowArgs :+= "optionSize=3"
    flowArgs :+= "option0=q:2"
    flowArgs :+= "option1=threshold:1" //0,1,2
    flowArgs :+= "option2=algorithm:EDJoin"
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_join"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_join.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assertResult(List(
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"),
        KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1001")), "", 0),
      Profile(1, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"),
        KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1006")), "", 0))
    )(items)
  }


  it should "call ERFlowLauncher batch-join" in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSBatchJoin"
    flowArgs :+= "dataSet1=" + TestDirs.resolveDataPath("flowdata/dt01.csv")
    flowArgs :+= "dataSet1-id=" + "t_id"
    flowArgs :+= "dataSet1-format=" + "csv"
    flowArgs :+= "dataSet1-attrSet=" + "t_pid"
    flowArgs :+= "dataSet1-filterSize=2"
    flowArgs :+= "dataSet1-filter0=site:CN"
    flowArgs :+= "dataSet1-filter1=t_date:20200715"
    flowArgs :+= "dataSet1-additionalAttrSet=t_pid"
    flowArgs :+= "dataSet2=" + TestDirs.resolveDataPath("flowdata/dp01.csv")
    flowArgs :+= "dataSet2-id=" + "p_id"
    flowArgs :+= "dataSet2-format=" + "csv"
    flowArgs :+= "dataSet2-attrSet=" + "p_id"
    flowArgs :+= "dataSet2-filterSize=1"
    flowArgs :+= "dataSet2-filter0=type:fund"
    flowArgs :+= "dataSet2-additionalAttrSet="
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs :+= "q=2"
    flowArgs :+= "optionSize=3"
    flowArgs :+= "option0=q:2"
    flowArgs :+= "option1=threshold:1" //0,1,2
    flowArgs :+= "option2=algorithm:EDJoin"
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_join"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_join.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assertResult(List(
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"),
        KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1001")), "", 0),
      Profile(1, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"),
        KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1006")), "", 0))
    )(items)
  }

}
