package org.wumiguo.ser

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.ProfilesListComparison._
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

  it should "call ERFlowLauncher SSJoinPL " in {
    val resFile = "tp_ss_join_pl"
    val flowType = "SSJoinPosL"
    var flowArgs: Array[String] = prepareEdJoinOnSingleAttrFlowArgs(resFile, flowType)
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/" + resFile + ".csv"
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

  it should "call ERFlowLauncher SSJoin  - enabled connected clustering" in {
    val flowType = "SSJoin"
    val resFile = "tp_ss_join_ecc"
    var flowArgs: Array[String] = prepareEdJoinOnSingleAttrFlowArgs(resFile, flowType)
    flowArgs :+= "connectedClustering=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/" + resFile + ".csv"
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


  it should "call ERFlowLauncher SSJoin " in {
    val flowType = "SSJoin"
    val resFile = "tp_ss_join_dcc"
    var flowArgs: Array[String] = prepareEdJoinOnSingleAttrFlowArgs(resFile, flowType)
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/" + resFile + ".csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assertResult(2)(items.size)
    assert(sameIgnoreOrder(List(
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P2-ID", "PG10091")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1001")), "", 0))
      , items.map(p => {
        Profile(0, p.attributes, p.originalID, p.sourceId)
      })))
  }

  it should "call ERFlowLauncher para-join" in {
    val flowType = "SSParaJoin"
    val resFile = "tp_para_join"
    var flowArgs: Array[String] = prepareEdJoinOnSingleAttrFlowArgs(resFile, flowType)
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
    flowArgs ++= prepare2DataSetWithIdProvidedAndSingleAttrJoinConf()
    flowArgs ++= prepareQ2T1EDJFlowOpts()
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
    flowArgs ++= prepare2DataSetWithIdProvidedAndSingleAttrJoinConf()
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs ++= prepareQ2T1EDJFlowOpts()
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
    val flowType = "SSJoin"
    val resFile = "tp_join2"
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=" + flowType
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
    flowArgs ++= prepareQ2T1EDJFlowOpts()
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + resFile
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    flowArgs :+= "connectedClustering=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/" + resFile + ".csv"
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


  it should "call ERFlowLauncher SSJoin disabled connected clustering v2" in {
    val flowType = "SSJoin"
    val resFile = "tp_join_dcc_2"
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=" + flowType
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
    flowArgs ++= prepareQ2T1EDJFlowOpts()
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + resFile
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    flowArgs :+= "connectedClustering=" + "false"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/" + resFile + ".csv"
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
        KeyValue("P1-system_id", "TENCGG"), KeyValue("P2-ID", "PU1001"), KeyValue("P2-p_name", "FinTechETF")), "", 0)
    )
    ))
  }
  it should "call ERFlowLauncher SSJoinPosL v2" in {
    val flowType = "SSJoinPosL"
    val resFile = "tp_join2_posl"
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=" + flowType
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
    flowArgs ++= prepareQ2T1EDJFlowOpts()
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + resFile
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/" + resFile + ".csv"
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


  private def prepareEdJoinOnSingleAttrFlowArgs(resFile: String, flowType: String) = {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=" + flowType
    flowArgs ++= prepare2DataSetWithIdProvidedAndSingleAttrJoinConf()
    flowArgs ++= prepareQ2T1EDJFlowOpts()
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + resFile
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    flowArgs
  }

  private def prepare2DataSetWithIdProvidedAndSingleAttrJoinConf() = {
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
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs
  }


  private def prepareQ2T1EDJFlowOpts() = {
    var flowArgs = Array[String]()
    flowArgs :+= "optionSize=3"
    flowArgs :+= "option0=q:2"
    flowArgs :+= "option1=threshold:1" //0,1,2
    flowArgs :+= "option2=algorithm:EDJoin"
    flowArgs
  }
}
