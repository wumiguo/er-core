package org.wumiguo.ser.flow.configuration

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.SparkTestingEnvSetup
import org.wumiguo.ser.dataloader.{CSVProfileLoader, DataTypeResolver, ProfileLoaderFactory}
import org.wumiguo.ser.dataloader.filter.SpecificFieldValueFilter
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}
import org.wumiguo.ser.testutil.TestDirs

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/9/22
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class DataSetConfigurationTest extends AnyFlatSpec with SparkTestingEnvSetup {
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
    assert(dsConfig.includeRealID)
    assertResult(DataSetConfiguration("/data-input/a.parq", "a_id",
      Seq("a_id", "sys"), Seq("date", "sys", "user"), Seq[KeyValue]()))(dsConfig)
    val dsConfig2 = CommandLineConfigLoader.load(args, "dataSet2")
    assert(dsConfig2.includeRealID)
    assertResult(DataSetConfiguration("/data-input/curr.parq", "c_id",
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


  it should "use csv profile loader to load" in {
    val path = TestDirs.resolveTestResourcePath("data/csv/dt01.min.csv")
    val dataSetConfig = DataSetConfiguration(path, "t_id", Seq("t_pid"), Seq("system_id"))
    val loader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(path))
    assert(loader.isInstanceOf[CSVProfileLoader.type])
    assert(!dataSetConfig.includeRealID)
    val epStartID = 0
    val sourceId = 1
    val data = loader.load(
      path, realIDField = dataSetConfig.idField,
      startIDFrom = epStartID,
      sourceId = sourceId, keepRealID = dataSetConfig.includeRealID,
      fieldsToKeep = (dataSetConfig.joinAttrs.toList ++ dataSetConfig.additionalAttrs),
      fieldValuesScope = dataSetConfig.filterOptions.toList,
      filter = SpecificFieldValueFilter)
    val expect = List(
      Profile(0, mutable.MutableList(KeyValue("t_pid", "P1007"), KeyValue("system_id", "ALIBB")), "TCN001277", 1),
      Profile(1, mutable.MutableList(KeyValue("t_pid", "U1001"), KeyValue("system_id", "TENCGG")), "TCN001278", 1),
      Profile(2, mutable.MutableList(KeyValue("t_pid", "S004"), KeyValue("system_id", "TENCGG")), "TCN001279", 1))
    assertResult(expect)(data.collect().toList)
  }

  it should "use csv profile loader to load without id" in {
    val path = TestDirs.resolveTestResourcePath("data/csv/dt01.min.csv")
    val dataSetConfig = DataSetConfiguration(path, "", Seq("t_pid"), Seq("system_id"))
    val loader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(path))
    assert(loader.isInstanceOf[CSVProfileLoader.type])
    assert(!dataSetConfig.includeRealID)
    val epStartID = 0
    val sourceId = 1
    val data = loader.load(
      path, realIDField = dataSetConfig.idField,
      startIDFrom = epStartID,
      sourceId = sourceId, keepRealID = dataSetConfig.includeRealID,
      fieldsToKeep = (dataSetConfig.joinAttrs.toList ++ dataSetConfig.additionalAttrs),
      fieldValuesScope = dataSetConfig.filterOptions.toList,
      filter = SpecificFieldValueFilter)
    val expect = List(
      Profile(0, mutable.MutableList(KeyValue("t_pid", "P1007"), KeyValue("system_id", "ALIBB")), "", 1),
      Profile(1, mutable.MutableList(KeyValue("t_pid", "U1001"), KeyValue("system_id", "TENCGG")), "", 1),
      Profile(2, mutable.MutableList(KeyValue("t_pid", "S004"), KeyValue("system_id", "TENCGG")), "", 1))
    assertResult(expect)(data.collect().toList)
  }


  it should "use csv profile loader to load with id as joinAttr" in {
    val path = TestDirs.resolveTestResourcePath("data/csv/dt01.min.csv")
    val dataSetConfig = DataSetConfiguration(path, "", Seq("t_id", "t_pid"), Seq("system_id"))
    val loader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(path))
    assert(!dataSetConfig.includeRealID)
    val epStartID = 0
    val sourceId = 1
    val data = loader.load(
      path, realIDField = dataSetConfig.idField,
      startIDFrom = epStartID,
      sourceId = sourceId, keepRealID = dataSetConfig.includeRealID,
      fieldsToKeep = (dataSetConfig.joinAttrs.toList ++ dataSetConfig.additionalAttrs),
      fieldValuesScope = dataSetConfig.filterOptions.toList,
      filter = SpecificFieldValueFilter)
    val expect = List(
      Profile(0, mutable.MutableList(KeyValue("t_id", "TCN001277"), KeyValue("t_pid", "P1007"), KeyValue("system_id", "ALIBB")), "", 1),
      Profile(1, mutable.MutableList(KeyValue("t_id", "TCN001278"), KeyValue("t_pid", "U1001"), KeyValue("system_id", "TENCGG")), "", 1),
      Profile(2, mutable.MutableList(KeyValue("t_id", "TCN001279"), KeyValue("t_pid", "S004"), KeyValue("system_id", "TENCGG")), "", 1))
    assertResult(expect)(data.collect().toList)
  }


  it should "use csv profile loader to load with id as additionalAttr" in {
    val path = TestDirs.resolveTestResourcePath("data/csv/dt01.min.csv")
    val dataSetConfig = DataSetConfiguration(path, "", Seq("t_pid"), Seq("t_id", "system_id"))
    val loader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(path))
    assert(!dataSetConfig.includeRealID)
    val epStartID = 0
    val sourceId = 1
    val data = loader.load(
      path, realIDField = dataSetConfig.idField,
      startIDFrom = epStartID,
      sourceId = sourceId, keepRealID = dataSetConfig.includeRealID,
      fieldsToKeep = (dataSetConfig.joinAttrs.toList ++ dataSetConfig.additionalAttrs),
      fieldValuesScope = dataSetConfig.filterOptions.toList,
      filter = SpecificFieldValueFilter)
    val expect = List(
      Profile(0, mutable.MutableList(KeyValue("t_id", "TCN001277"), KeyValue("t_pid", "P1007"), KeyValue("system_id", "ALIBB")), "", 1),
      Profile(1, mutable.MutableList(KeyValue("t_id", "TCN001278"), KeyValue("t_pid", "U1001"), KeyValue("system_id", "TENCGG")), "", 1),
      Profile(2, mutable.MutableList(KeyValue("t_id", "TCN001279"), KeyValue("t_pid", "S004"), KeyValue("system_id", "TENCGG")), "", 1))
    assertResult(expect)(data.collect().toList)
  }


  it should "use csv profile loader to load using additional id" in {
    val path = TestDirs.resolveTestResourcePath("data/csv/dt01.min.csv")
    val dataSetConfig = DataSetConfiguration(path, "t_id", Seq("t_id", "t_pid"), Seq("system_id"))
    val loader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(path))
    assert(dataSetConfig.includeRealID)
    val epStartID = 0
    val sourceId = 1
    val data = loader.load(
      path, realIDField = dataSetConfig.idField,
      startIDFrom = epStartID,
      sourceId = sourceId, keepRealID = dataSetConfig.includeRealID,
      fieldsToKeep = (dataSetConfig.joinAttrs.toList ++ dataSetConfig.additionalAttrs),
      fieldValuesScope = dataSetConfig.filterOptions.toList,
      filter = SpecificFieldValueFilter)
    val expect = List(
      Profile(0, mutable.MutableList(KeyValue("t_id", "TCN001277"), KeyValue("t_pid", "P1007"), KeyValue("system_id", "ALIBB")), "TCN001277", 1),
      Profile(1, mutable.MutableList(KeyValue("t_id", "TCN001278"), KeyValue("t_pid", "U1001"), KeyValue("system_id", "TENCGG")), "TCN001278", 1),
      Profile(2, mutable.MutableList(KeyValue("t_id", "TCN001279"), KeyValue("t_pid", "S004"), KeyValue("system_id", "TENCGG")), "TCN001279", 1))
    assertResult(expect)(data.collect().toList)
  }

}
