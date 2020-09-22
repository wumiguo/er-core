package org.wumiguo.ser.flow.configuration

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.methods.datastructure.KeyValue

/**
 * @author levinliu
 *         Created on 2020/9/22
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class DataSetConfigurationTest extends AnyFlatSpec {
  it should "load data set config" in {
    var args = Array[String]()
    args :+= "dataSet1=/data-input/a.parq"
    args :+= "dataSet1-id=a_id"
    args :+= "dataSet1-attrSet=a_id,sys"
    args :+= "dataSet1-additionalAttrSet=date,sys,user"
    args :+= "datSet1-id=dd_id"
    args :+= "datSet1-attrSet=dd_id,sys"
    args :+= "datSet1-additionalAttrSet=date,sys,user2"
    args :+= "dataSet2=/data-input/curr.parq"
    args :+= "dataSet2-id=c_id"
    args :+= "dataSet2-attrSet=c_id,site"
    args :+= "dataSet2-additionalAttrSet=date,sys,remark"
    args :+= "dataSet2-filterSize=1"
    args :+= "dataSet2-filter0=date:20200101"
    val dsConfig = CommandLineConfigLoader.load(args, "dataSet1")
    assertResult(DataSetConfiguration("/data-input/a.parq", "a_id",
      Seq("a_id", "sys"), Seq("date", "sys", "user"), Seq[KeyValue]()))(dsConfig)
    val dsConfig2 = CommandLineConfigLoader.load(args, "dataSet2")
    assertResult(DataSetConfiguration("/data-input/curr.parq", "c_id",
      Seq("c_id", "site"), Seq("date", "sys", "remark"), Seq[KeyValue](KeyValue("date", "20200101"))))(dsConfig2)
  }

}
