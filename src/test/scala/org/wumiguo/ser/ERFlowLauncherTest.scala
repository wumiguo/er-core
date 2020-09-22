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

  it should "call ERFlowLauncher SSJoin " in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSJoin"
    flowArgs ++= prepareDSConf()
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs ++= prepareFlowOpts()
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_ss_join"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_ss_join.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assertResult(3)(items.size)
    assert(sameIgnoreOrder(items.map(p => {
      Profile(0, p.attributes, p.originalID, p.sourceId)
    }), List(
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P2-ID", "PG10091")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1001")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1006")), "", 0))
    ))
  }

  def sameIgnoreOrder(p1List: List[Profile], p2List: List[Profile]): Boolean = {
    if (p1List.size == p2List.size) {
      var counter = 0
      for (p1 <- p1List) {
        for (p2 <- p2List) {
          if (p1 == p2) {
            counter = counter + 1
          }
        }
      }
      val result = counter == p1List.size
      if (!result) {
        assertResult(p1List)(p2List)
      }
      result
    } else {
      assertResult(p1List)(p2List)
      false
    }
  }

  it should "call ERFlowLauncher para-join" in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSParaJoin"
    flowArgs ++= prepareDSConf()
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs ++= prepareFlowOpts()
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_para_join"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_para_join.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assert(sameIgnoreOrder(items.map(p => {
      Profile(0, p.attributes, p.originalID, p.sourceId)
    }), List(
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P2-ID", "PG10091")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1001")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1006")), "", 0))
    ))
  }


  it should "call ERFlowLauncher batch-join" in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSBatchJoin"
    flowArgs ++= prepareDSConf()
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs ++= prepareFlowOpts()
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_join_batch"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_join_batch.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assert(sameIgnoreOrder(items.map(p => {
      Profile(0, p.attributes, p.originalID, p.sourceId)
    }), List(
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P2-ID", "PG10091")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1001")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1006")), "", 0))
    ))
  }


  it should "call ERFlowLauncher batch-V2-join" in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSBatchV2Join"
    flowArgs ++= prepareDSConf()
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs ++= prepareFlowOpts()
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_join_batchv2"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_join_batchv2.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assert(sameIgnoreOrder(items.map(p => {
      Profile(0, p.attributes, p.originalID, p.sourceId)
    }), List(
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P2-ID", "PG10091")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "0.8333333333333334"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1001")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0E-5"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1006")), "", 0))
    ))
  }

  it should "call ERFlowLauncher SSJoin v2" in {
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
    flowArgs ++= prepareFlowOpts()
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
    assert(sameIgnoreOrder(items.map(p => {
      Profile(0, p.attributes, p.originalID, p.sourceId)
    }), List(
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"),
        KeyValue("P1-system_id", "CTENCGG"), KeyValue("P2-ID", "PG10091"), KeyValue("P2-p_name", "TECF1")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"),
        KeyValue("P1-system_id", "TENCGG"), KeyValue("P2-ID", "PU1001"), KeyValue("P2-p_name", "FinTechETF")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"),
        KeyValue("P1-system_id", "TENCGG"), KeyValue("P2-ID", "PU1006"), KeyValue("P2-p_name", "FunStock")), "", 0)
    )
    ))
  }


  it should "call ERFlowLauncher SSBatchV2Join v2" in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSBatchV2Join"
    flowArgs :+= "dataSet1=" + TestDirs.resolveDataPath("flowdata/dt01.csv")
    flowArgs :+= "dataSet1-id=" + "t_id"
    flowArgs :+= "dataSet1-format=" + "csv"
    flowArgs :+= "dataSet1-attrSet=" + "t_pid,system_id"
    flowArgs :+= "dataSet1-filterSize=2"
    flowArgs :+= "dataSet1-filter0=site:CN"
    flowArgs :+= "dataSet1-filter1=t_date:20200715"
    flowArgs :+= "dataSet1-additionalAttrSet=t_pid,system_id"
    flowArgs :+= "dataSet2=" + TestDirs.resolveDataPath("flowdata/dp01.csv")
    flowArgs :+= "dataSet2-id=" + "p_id"
    flowArgs :+= "dataSet2-format=" + "csv"
    flowArgs :+= "dataSet2-attrSet=" + "p_id,system_id"
    flowArgs :+= "dataSet2-filterSize=1"
    flowArgs :+= "dataSet2-filter0=type:fund"
    flowArgs :+= "dataSet2-additionalAttrSet=p_name,system_id"
    flowArgs :+= "joinFieldsWeight=0.7,0.3"
    flowArgs :+= "optionSize=3"
    flowArgs :+= "option0=q:2"
    flowArgs :+= "option1=threshold:0" //0,1,2
    flowArgs :+= "option2=algorithm:EDJoin"
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_joinv2_2"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_joinv2_2.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assert(sameIgnoreOrder(items.map(p => {
      Profile(0, p.attributes, p.originalID, p.sourceId)
    }), List(
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P1-system_id", "CTENCGG"), KeyValue("P2-ID", "PG10091"), KeyValue("P2-p_name", "TECF1"), KeyValue("P2-system_id", "CTENCGG")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "0.3"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P1-system_id", "CTENCGG"), KeyValue("P2-ID", "PB2004"), KeyValue("P2-p_name", "SMETF"), KeyValue("P2-system_id", "CTENCGG")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "0.3"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P1-system_id", "CTENCGG"), KeyValue("P2-ID", "PG10101"), KeyValue("P2-p_name", "TECF2"), KeyValue("P2-system_id", "CTENCGG")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "0.3"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P1-system_id", "CTENCGG"), KeyValue("P2-ID", "PU1001"), KeyValue("P2-p_name", "FinTechETF"), KeyValue("P2-system_id", "CTENCGG")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "0.3"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P1-system_id", "CTENCGG"), KeyValue("P2-ID", "PE1003"), KeyValue("P2-p_name", "BCTech"), KeyValue("P2-system_id", "CTENCGG")), "", 0)
    )))
  }

  private def prepareDSConf() = {
    var flowArgs: Array[String] = Array()
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
    flowArgs
  }


  private def prepareFlowOpts() = {
    var flowArgs = Array[String]()
    flowArgs :+= "optionSize=3"
    flowArgs :+= "option0=q:2"
    flowArgs :+= "option1=threshold:1" //0,1,2
    flowArgs :+= "option2=algorithm:EDJoin"
    flowArgs
  }
}
