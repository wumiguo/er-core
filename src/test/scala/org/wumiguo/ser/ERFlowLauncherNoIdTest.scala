package org.wumiguo.ser

import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.dataloader.{DataTypeResolver, ProfileLoaderFactory}
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}
import org.wumiguo.ser.testutil.{CollectionsComparision, SparkTestingEnvSetup, TestDirs}

import scala.collection.mutable
import scala.reflect.io.File

/**
 * @author levinliu
 *         Created on 2020/9/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class ERFlowLauncherNoIdTest extends AnyFlatSpec with SparkTestingEnvSetup {

  it should "call ERFlowLauncher SSJoin - no id" in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSJoin"
    flowArgs :+= "dataSet1=" + TestDirs.resolveDataPath("flowdata/dt01.csv")
    flowArgs :+= "dataSet1-id=" + ""
    flowArgs :+= "dataSet1-format=" + "csv"
    flowArgs :+= "dataSet1-attrSet=" + "t_pid"
    flowArgs :+= "dataSet1-filterSize=2"
    flowArgs :+= "dataSet1-filter0=site:CN"
    flowArgs :+= "dataSet1-filter1=t_date:20200715"
    flowArgs :+= "dataSet1-additionalAttrSet=t_pid"
    flowArgs :+= "dataSet2=" + TestDirs.resolveDataPath("flowdata/dp01.csv")
    flowArgs :+= "dataSet2-id=" + ""
    flowArgs :+= "dataSet2-format=" + "csv"
    flowArgs :+= "dataSet2-attrSet=" + "p_id"
    flowArgs :+= "dataSet2-filterSize=1"
    flowArgs :+= "dataSet2-filter0=type:fund"
    flowArgs :+= "dataSet2-additionalAttrSet="
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs ++= prepareFlowOpts()
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_ss_join_no_id"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    flowArgs :+= "connectedClustering=" + "false"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_ss_join_no_id.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assertResult(2)(items.size)
    CollectionsComparision.assertSameIgnoreOrder(
      List(
        Profile(0, mutable.MutableList(KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "4"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P2-ID", "7")), "", 0),
        Profile(0, mutable.MutableList(KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "0"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "0")), "", 0))
      , items.map(p => Profile(0, p.attributes, p.originalID, p.sourceId))
    )
  }

  it should "call ERFlowLauncher SSJoin - show ID " in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSJoin"
    flowArgs :+= "dataSet1=" + TestDirs.resolveDataPath("flowdata/dt01.csv")
    flowArgs :+= "dataSet1-id=" + ""
    flowArgs :+= "dataSet1-format=" + "csv"
    flowArgs :+= "dataSet1-attrSet=" + "t_pid"
    flowArgs :+= "dataSet1-filterSize=2"
    flowArgs :+= "dataSet1-filter0=site:CN"
    flowArgs :+= "dataSet1-filter1=t_date:20200715"
    flowArgs :+= "dataSet1-additionalAttrSet=t_id,t_pid"
    flowArgs :+= "dataSet2=" + TestDirs.resolveDataPath("flowdata/dp01.csv")
    flowArgs :+= "dataSet2-id=" + ""
    flowArgs :+= "dataSet2-format=" + "csv"
    flowArgs :+= "dataSet2-attrSet=" + "p_id"
    flowArgs :+= "dataSet2-filterSize=1"
    flowArgs :+= "dataSet2-filter0=type:fund"
    flowArgs :+= "dataSet2-additionalAttrSet=p_id"
    flowArgs :+= "joinFieldsWeight=1.0"
    flowArgs ++= prepareFlowOpts()
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_ss_join_show_id"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    flowArgs :+= "connectedClustering=" + "false"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_ss_join_show_id.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assertResult(2)(items.size)
    CollectionsComparision.assertSameIgnoreOrder(List(
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "4"), KeyValue("P1-t_id", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P2-ID", "7"), KeyValue("P2-p_id", "PG10091")), "", 0),
      Profile(0, mutable.MutableList(
        KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "0"), KeyValue("P1-t_id", "TCN001278"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "0"), KeyValue("P2-p_id", "PU1001")), "", 0))
      , items.map(p => Profile(0, p.attributes, p.originalID, p.sourceId)))
  }

  it should "call ERFlowLauncher SSJoin - with ID " in {
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
    flowArgs ++= prepareFlowOpts()
    flowArgs :+= "outputPath=" + TestDirs.resolveOutputPath("trade-product")
    flowArgs :+= "outputType=" + "csv"
    flowArgs :+= "joinResultFile=" + "tp_ss_join_use_id"
    flowArgs :+= "overwriteOnExist=" + "true"
    flowArgs :+= "showSimilarity=" + "true"
    flowArgs :+= "connectedClustering=" + "false"
    ERFlowLauncher.main(flowArgs)
    val outputPath = TestDirs.resolveOutputPath("trade-product") + "/tp_ss_join_use_id.csv"
    val outputFile: File = File(outputPath)
    assertResult(true)(outputFile.exists)
    val out = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(outputPath)).load(outputPath)
    val items = out.collect.toList
    assertResult(2)(items.size)
    CollectionsComparision.assertSameIgnoreOrder(
      List(
        Profile(0, mutable.MutableList(
          KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001312"), KeyValue("P1-t_pid", "PG10091"), KeyValue("P2-ID", "PG10091")), "", 0),
        Profile(0, mutable.MutableList(
          KeyValue("Similarity", "1.0"), KeyValue("P1-ID", "TCN001278"), KeyValue("P1-t_pid", "U1001"), KeyValue("P2-ID", "PU1001")), "", 0))
      , items.map(p => Profile(0, p.attributes, p.originalID, p.sourceId)
      ))
  }

  private def prepareDSConf() = {
    var flowArgs: Array[String] = Array()
    flowArgs :+= "dataSet1=" + TestDirs.resolveDataPath("flowdata/dt01.csv")
    flowArgs :+= "dataSet1-id=" + ""
    flowArgs :+= "dataSet1-format=" + "csv"
    flowArgs :+= "dataSet1-attrSet=" + "t_pid"
    flowArgs :+= "dataSet1-filterSize=2"
    flowArgs :+= "dataSet1-filter0=site:CN"
    flowArgs :+= "dataSet1-filter1=t_date:20200715"
    flowArgs :+= "dataSet1-additionalAttrSet=t_pid"
    flowArgs :+= "dataSet2=" + TestDirs.resolveDataPath("flowdata/dp01.csv")
    flowArgs :+= "dataSet2-id=" + ""
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
