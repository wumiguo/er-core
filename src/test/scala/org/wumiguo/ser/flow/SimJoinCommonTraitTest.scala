package org.wumiguo.ser.flow

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.flow.configuration.DataSetConfiguration
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}
import org.wumiguo.ser.testutil.{SparkTestingEnvSetup, TestDirs}

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/9/28
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class SimJoinCommonTraitTest extends AnyFlatSpec with SimJoinCommonTrait with SparkTestingEnvSetup {
  it should "loadDataWithOption" in {
    val path = TestDirs.resolveDataPath("csv/sample-rates.csv")
    val dataSetConf = DataSetConfiguration(path, "id", Seq("event"), Seq("date"))
    val profiles = loadDataWithOption(dataSetConf, 0, 4)
    assertResult(List(Profile(0, mutable.MutableList(KeyValue("event", "price up 1%")), "1", 4),
      Profile(1, mutable.MutableList(KeyValue("event", "price drop 0.5%")), "2", 4),
      Profile(2, mutable.MutableList(KeyValue("event", "price up 0.4%")), "3", 4)))(profiles.collect.toList)
  }
  it should "collectAttributesFromProfiles" in {
    val path = TestDirs.resolveDataPath("csv/sample-rates.csv")
    val dataSetConf = DataSetConfiguration(path, "id", Seq("event"), Seq("date"))
    val profiles = loadDataWithOption(dataSetConf, 0, 4)
    val attrPairArr = collectAttributesFromProfiles(profiles, profiles, dataSetConf, dataSetConf)
    assertResult(1)(attrPairArr.size)
  }
  it should "collectAttributesPairFromProfiles" in {
    val path = TestDirs.resolveDataPath("csv/sample-rates.csv")
    val dataSetConf = DataSetConfiguration(path, "id", Seq("event"), Seq("date"))
    val profiles = loadDataWithOption(dataSetConf, 0, 4)
    val attrPairTup = collectAttributesPairFromProfiles(profiles, profiles, dataSetConf, dataSetConf)
    assertResult(attrPairTup._1.map(x => (x._1, x._2.toList)).collect.toList)(attrPairTup._2.map(x => (x._1, x._2.toList)).collect.toList)
    assertResult(List(
      (0, List("price up 1%")),
      (1, List("price drop 0.5%")),
      (2, List("price up 0.4%")))
    )(attrPairTup._2.map(x => (x._1, x._2.toList)).collect.toList)
  }


  it should "loadDataInOneGo" in {
    val path = TestDirs.resolveDataPath("csv/sample-rates.csv")
    val dataSetConf = DataSetConfiguration(path, "id", Seq("event"), Seq("date"))
    val profiles = loadDataInOneGo(dataSetConf, 0, 4)
    assertResult(List(
      Profile(0, mutable.MutableList(KeyValue("event", "price up 1%"), KeyValue("date", "20200101")), "1", 4),
      Profile(1, mutable.MutableList(KeyValue("event", "price drop 0.5%"), KeyValue("date", "20200102")), "2", 4),
      Profile(2, mutable.MutableList(KeyValue("event", "price up 0.4%"), KeyValue("date", "20200103")), "3", 4)))(profiles.collect.toList)
    val attrPairTup = collectAttributesPairFromProfiles(profiles, profiles, dataSetConf, dataSetConf)
    assertResult(attrPairTup._1.map(x => (x._1, x._2.toList)).collect.toList)(attrPairTup._2.map(x => (x._1, x._2.toList)).collect.toList)
    assertResult(List(
      (0, List("price up 1%")),
      (1, List("price drop 0.5%")),
      (2, List("price up 0.4%")))
    )(attrPairTup._2.map(x => (x._1, x._2.toList)).collect.toList)
  }
}
