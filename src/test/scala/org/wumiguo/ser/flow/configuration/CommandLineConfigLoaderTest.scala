package org.wumiguo.ser.flow.configuration

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.SparkTestingEnvSetup
import org.wumiguo.ser.methods.datastructure.KeyValue
import org.wumiguo.ser.testutil.TestDirs

/**
 * @author levinliu
 *         Created on 2020/12/3
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class CommandLineConfigLoaderTest extends AnyFlatSpec with SparkTestingEnvSetup {
  it should "load data set config" in {
    var args = Array[String]()
    val dtPath = TestDirs.resolveDataPath("flowdata/dt01.csv")
    val dpPath = TestDirs.resolveDataPath("flowdata/dp01.csv")
    args :+= "dataSet1=" + dtPath
    args :+= "dataSet1-id=t_id"
    args :+= "dataSet1-attrSet=t_id,sys"
    args :+= "dataSet1-additionalAttrSet=date,sys,user"
    args :+= "datSet1-id=dd_id"
    args :+= "datSet1-attrSet=dd_id,sys"
    args :+= "datSet1-additionalAttrSet=date,sys,user2"
    args :+= "dataSet2=" + dpPath
    args :+= "dataSet2-id=c_id"
    args :+= "dataSet2-attrSet=c_id,site"
    args :+= "dataSet2-additionalAttrSet=date,sys,remark"
    args :+= "dataSet2-filterSize=1"
    args :+= "dataSet2-filter0=date:20200101"
    val dsConfig = CommandLineConfigLoader.load(args, "dataSet1")
    assert(dsConfig.includeRealID)
    assertResult(DataSetConfiguration(dtPath, "t_id",
      Seq("t_id", "sys"), Seq("date", "sys", "user"), Seq[KeyValue]()))(dsConfig)
    val dsConfig2 = CommandLineConfigLoader.load(args, "dataSet2")
    assert(dsConfig2.includeRealID)
    assertResult(DataSetConfiguration(dpPath, "c_id",
      Seq("c_id", "site"), Seq("date", "sys", "remark"), Seq[KeyValue](KeyValue("date", "20200101"))))(dsConfig2)
  }

  it should "load data set config - exclude id" in {
    var args = Array[String]()
    args :+= "dataSet1=/data-input/a.parq"
    args :+= "dataSet1-id="
    args :+= "dataSet1-attrSet=a_id,sys"
    args :+= "dataSet1-additionalAttrSet=date,sys,user"
    args :+= "datSet1-id=dd_id"
    args :+= "datSet1-attrSet=dd_id,sys"
    args :+= "datSet1-additionalAttrSet=date,sys,user2"
    args :+= "dataSet2=/data-input/curr.parq"
    args :+= "dataSet2-id="
    args :+= "dataSet2-attrSet=c_id,site"
    args :+= "dataSet2-additionalAttrSet=date,sys,remark"
    args :+= "dataSet2-filterSize=1"
    args :+= "dataSet2-filter0=date:20200101"
    val dsConfig = CommandLineConfigLoader.load(args, "dataSet1")
    assert(!dsConfig.includeRealID)
    assertResult(DataSetConfiguration("/data-input/a.parq", "",
      Seq("a_id", "sys"), Seq("date", "sys", "user"), Seq[KeyValue]()))(dsConfig)
    val dsConfig2 = CommandLineConfigLoader.load(args, "dataSet2")
    assert(!dsConfig2.includeRealID)
    assertResult(DataSetConfiguration("/data-input/curr.parq", "",
      Seq("c_id", "site"), Seq("date", "sys", "remark"), Seq[KeyValue](KeyValue("date", "20200101"))))(dsConfig2)
  }


}
