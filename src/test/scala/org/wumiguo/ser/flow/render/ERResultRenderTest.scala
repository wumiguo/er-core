package org.wumiguo.ser.flow.render

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.flow.configuration.DataSetConfiguration
import org.wumiguo.ser.flow.render.ERResultRender.{checkBothIdFieldsProvided, renderResultWithPreloadProfiles, resolveRowsV2, resolveColumns, adjustProfile2RelativeId}
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}
import org.wumiguo.ser.testutil.{CollectionsComparision, SparkTestingEnvSetup, TestDirs}

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/11/9
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class ERResultRenderTest extends AnyFlatSpec with SparkTestingEnvSetup {
  it should "checkIdProvided" in {
    val dataSet1: DataSetConfiguration = DataSetConfiguration("dt_path", "", List("t_pid"), List("t_id", "t_pid"), List(KeyValue("site", "CN"), KeyValue("t_date", "20200715")))
    val dataSet2: DataSetConfiguration = DataSetConfiguration("dp_path", "", List("p_id"), List("p_id"), List(KeyValue("type", "fund")))
    assert(!checkBothIdFieldsProvided(dataSet1, dataSet2))
    val dataSet3: DataSetConfiguration = DataSetConfiguration("dt_path", "t_pid", List("t_pid"), List("t_id", "t_pid"), List(KeyValue("site", "CN"), KeyValue("t_date", "20200715")))
    val dataSet4: DataSetConfiguration = DataSetConfiguration("dp_path", "p_id", List("p_id"), List("p_id"), List(KeyValue("type", "fund")))
    assert(checkBothIdFieldsProvided(dataSet3, dataSet4))
  }

  it should "adjustProfile2RelativeId" in {
    assertResult("3")(adjustProfile2RelativeId("7", 4, false))
    assertResult("2")(adjustProfile2RelativeId("7", 5, false))
    assertResult("7")(adjustProfile2RelativeId("7", 5, true))
  }

  it should "resolveColumns" in {
    val moreAttrs1 = List("t_id", "t_pid")
    val moreAttrs2 = List("p_id")
    assertResult(Seq("Similarity", "P1-ID", "P1-t_id", "P1-t_pid", "P2-ID", "P2-p_id"))(resolveColumns(moreAttrs1, moreAttrs2, true))
    assertResult(Seq("P1-ID", "P1-t_id", "P1-t_pid", "P2-ID", "P2-p_id"))(resolveColumns(moreAttrs1, moreAttrs2, false))
  }

  it should "resolveRowsV2" in {
    val finalMap = spark.sparkContext.makeRDD(Seq(
      (0, 5, 0.1), (2, 4, 0.8)))
    val dataSet1: DataSetConfiguration = DataSetConfiguration("dt_path", "", List("t_pid"), List("t_id", "t_pid"), List(KeyValue("site", "CN"), KeyValue("t_date", "20200715")))
    val dataSet2: DataSetConfiguration = DataSetConfiguration("dp_path", "", List("p_id"), List("p_id"), List(KeyValue("type", "fund")))
    val secondEPStartID = 3
    val showSimilarity = true
    val p1B = spark.sparkContext.broadcast(Array(
      Profile(0, mutable.MutableList(KeyValue("t_id", "TCN001278"), KeyValue("t_pid", "U1001")), "", 0),
      Profile(1, mutable.MutableList(KeyValue("t_id", "TCN001279"), KeyValue("t_pid", "S004")), "", 0),
      Profile(2, mutable.MutableList(KeyValue("t_id", "TCN001281"), KeyValue("t_pid", "AS003")), "", 0)
    ))
    val p2B = spark.sparkContext.broadcast(Array(
      Profile(3, mutable.MutableList(KeyValue("p_id", "PU1001")), "", 1),
      Profile(4, mutable.MutableList(KeyValue("p_id", "PA1002")), "", 1),
      Profile(5, mutable.MutableList(KeyValue("p_id", "PE1003")), "", 1)
    ))
    val idFieldProvided = checkBothIdFieldsProvided(dataSet1, dataSet2)
    val rows = resolveRowsV2(finalMap, idFieldProvided, dataSet1, dataSet2,
      p1B, p2B, secondEPStartID, showSimilarity)
    val result = rows.map(row => row.toSeq.map(item => item.toString).toList).collect.toList
    assertResult(List(
      List("0.1", "0", "TCN001278", "U1001", "2", "PE1003"),
      List("0.8", "2", "TCN001281", "AS003", "1", "PA1002")))(result)
  }
  it should "renderResultWithPreloadProfiles - no id fields" in {
    val dt = TestDirs.resolveDataPath("flowdata/dt01.csv")
    val dp = TestDirs.resolveDataPath("flowdata/dp01.csv")
    val dataSet1: DataSetConfiguration = DataSetConfiguration(dt, "", List("t_pid"), List("t_id", "t_pid"), List(KeyValue("site", "CN"), KeyValue("t_date", "20200715")))
    val dataSet2: DataSetConfiguration = DataSetConfiguration(dp, "", List("p_id"), List("p_id"), List(KeyValue("type", "fund")))
    val secondEPStartID = 5
    val matchDetails = spark.sparkContext.makeRDD(Seq(
      (4, 12, 1.0), (2, 3, 0.5), (0, 5, 0.5), (13, 14, 0.5), (5, 10, 0.5), (6, 11, 0.5), (9, 11, 0.5), (6, 9, 0.5)
    ))
    val profiles: RDD[Profile] = spark.sparkContext.makeRDD(List(
      Profile(0, mutable.MutableList(KeyValue("t_id", "TCN001278"), KeyValue("t_pid", "U1001")), "", 0),
      Profile(1, mutable.MutableList(KeyValue("t_id", "TCN001279"), KeyValue("t_pid", "S004")), "", 0),
      Profile(2, mutable.MutableList(KeyValue("t_id", "TCN001281"), KeyValue("t_pid", "AS003")), "", 0),
      Profile(3, mutable.MutableList(KeyValue("t_id", "TCN001301"), KeyValue("t_pid", "AS002")), "", 0),
      Profile(4, mutable.MutableList(KeyValue("t_id", "TCN001312"), KeyValue("t_pid", "PG10091")), "", 0),
      Profile(5, mutable.MutableList(KeyValue("p_id", "PU1001")), "", 1),
      Profile(6, mutable.MutableList(KeyValue("p_id", "PA1002")), "", 1),
      Profile(7, mutable.MutableList(KeyValue("p_id", "PE1003")), "", 1),
      Profile(8, mutable.MutableList(KeyValue("p_id", "PB2004")), "", 1),
      Profile(9, mutable.MutableList(KeyValue("p_id", "PA1005")), "", 1),
      Profile(10, mutable.MutableList(KeyValue("p_id", "PU1006")), "", 1),
      Profile(11, mutable.MutableList(KeyValue("p_id", "PA1007")), "", 1),
      Profile(12, mutable.MutableList(KeyValue("p_id", "PG10091")), "", 1),
      Profile(13, mutable.MutableList(KeyValue("p_id", "PG10101")), "", 1),
      Profile(14, mutable.MutableList(KeyValue("p_id", "PG10102")), "", 1)
    ))
    val matchedPairs: RDD[(Int, Int)] = spark.sparkContext.makeRDD(List(
      (4, 12), (0, 5), (2, 3), (13, 14), (6, 11), (9, 11), (6, 9), (5, 10)
    ))
    val showSimilarity: Boolean = true
    val profiles1: RDD[Profile] = spark.sparkContext.makeRDD(List(
      Profile(0, mutable.MutableList(KeyValue("t_id", "TCN001278"), KeyValue("t_pid", "U1001")), "", 0),
      Profile(1, mutable.MutableList(KeyValue("t_id", "TCN001279"), KeyValue("t_pid", "S004")), "", 0),
      Profile(2, mutable.MutableList(KeyValue("t_id", "TCN001281"), KeyValue("t_pid", "AS003")), "", 0),
      Profile(3, mutable.MutableList(KeyValue("t_id", "TCN001301"), KeyValue("t_pid", "AS002")), "", 0),
      Profile(4, mutable.MutableList(KeyValue("t_id", "TCN001312"), KeyValue("t_pid", "PG10091")), "", 0)
    ))
    val profiles2: RDD[Profile] = spark.sparkContext.makeRDD(List(
      Profile(5, mutable.MutableList(KeyValue("p_id", "PU1001")), "", 1),
      Profile(6, mutable.MutableList(KeyValue("p_id", "PA1002")), "", 1),
      Profile(7, mutable.MutableList(KeyValue("p_id", "PE1003")), "", 1),
      Profile(8, mutable.MutableList(KeyValue("p_id", "PB2004")), "", 1),
      Profile(9, mutable.MutableList(KeyValue("p_id", "PA1005")), "", 1),
      Profile(10, mutable.MutableList(KeyValue("p_id", "PU1006")), "", 1),
      Profile(11, mutable.MutableList(KeyValue("p_id", "PA1007")), "", 1),
      Profile(12, mutable.MutableList(KeyValue("p_id", "PG10091")), "", 1),
      Profile(13, mutable.MutableList(KeyValue("p_id", "PG10101")), "", 1),
      Profile(14, mutable.MutableList(KeyValue("p_id", "PG10102")), "", 1)
    ))
    val (columnNames, rows) = renderResultWithPreloadProfiles(
      dataSet1, dataSet2, secondEPStartID,
      profiles, matchedPairs,
      showSimilarity, profiles1, profiles2
    )
    assertResult(Seq("Similarity", "P1-ID", "P1-t_id", "P1-t_pid", "P2-ID", "P2-p_id"))(columnNames)
    CollectionsComparision.assertSameIgnoreOrder(List(
      List("1.0", "4", "TCN001312", "PG10091", "7", "PG10091"),
      List("1.0", "0", "TCN001278", "U1001", "0", "PU1001")
    ),
      rows.collect.map(x => x.toSeq.toList).toList)
  }

  it should "renderResultWithPreloadProfiles - idFieldsProvided" in {
    val dt = TestDirs.resolveDataPath("flowdata/dt01.csv")
    val dp = TestDirs.resolveDataPath("flowdata/dp01.csv")
    val dataSet1: DataSetConfiguration = DataSetConfiguration(dt, "t_id", List("t_pid"), List("t_pid"), List(KeyValue("site", "CN"), KeyValue("t_date", "20200715")))
    val dataSet2: DataSetConfiguration = DataSetConfiguration(dp, "p_id", List("p_id"), List(), List(KeyValue("type", "fund")))
    val secondEPStartID = 5
    val matchDetails = spark.sparkContext.makeRDD(
      List((4, 12, 1.0), (0, 5, 0.5), (2, 3, 0.5), (13, 14, 0.5), (6, 11, 0.5), (9, 11, 0.5), (6, 9, 0.5), (5, 10, 0.5))
    )
    val profiles: RDD[Profile] = spark.sparkContext.makeRDD(List(
      Profile(0, mutable.MutableList(KeyValue("t_pid", "U1001")), "TCN001278", 0),
      Profile(1, mutable.MutableList(KeyValue("t_pid", "S004")), "TCN001279", 0),
      Profile(2, mutable.MutableList(KeyValue("t_pid", "AS003")), "TCN001281", 0),
      Profile(3, mutable.MutableList(KeyValue("t_pid", "AS002")), "TCN001301", 0),
      Profile(4, mutable.MutableList(KeyValue("t_pid", "PG10091")), "TCN001312", 0),
      Profile(5, mutable.MutableList(KeyValue("p_id", "PU1001")), "PU1001", 1),
      Profile(6, mutable.MutableList(KeyValue("p_id", "PA1002")), "PA1002", 1),
      Profile(7, mutable.MutableList(KeyValue("p_id", "PE1003")), "PE1003", 1),
      Profile(8, mutable.MutableList(KeyValue("p_id", "PB2004")), "PB2004", 1),
      Profile(9, mutable.MutableList(KeyValue("p_id", "PA1005")), "PA1005", 1),
      Profile(10, mutable.MutableList(KeyValue("p_id", "PU1006")), "PU1006", 1),
      Profile(11, mutable.MutableList(KeyValue("p_id", "PA1007")), "PA1007", 1),
      Profile(12, mutable.MutableList(KeyValue("p_id", "PG10091")), "PG10091", 1),
      Profile(13, mutable.MutableList(KeyValue("p_id", "PG10101")), "PG10101", 1),
      Profile(14, mutable.MutableList(KeyValue("p_id", "PG10102")), "PG10102", 1)))
    val matchedPairs: RDD[(Int, Int)] = spark.sparkContext.makeRDD(List((4, 12), (0, 5), (2, 3), (13, 14), (6, 11), (9, 11), (6, 9), (5, 10)))
    val showSimilarity: Boolean = true
    val profiles1: RDD[Profile] = spark.sparkContext.makeRDD(
      List(Profile(0, mutable.MutableList(KeyValue("t_pid", "U1001")), "TCN001278", 0),
        Profile(1, mutable.MutableList(KeyValue("t_pid", "S004")), "TCN001279", 0), Profile(2, mutable.MutableList(KeyValue("t_pid", "AS003")), "TCN001281", 0), Profile(3, mutable.MutableList(KeyValue("t_pid", "AS002")), "TCN001301", 0), Profile(4, mutable.MutableList(KeyValue("t_pid", "PG10091")), "TCN001312", 0))
    )
    val profiles2: RDD[Profile] = spark.sparkContext.makeRDD(
      List(Profile(5, mutable.MutableList(KeyValue("p_id", "PU1001")), "PU1001", 1),
        Profile(6, mutable.MutableList(KeyValue("p_id", "PA1002")), "PA1002", 1),
        Profile(7, mutable.MutableList(KeyValue("p_id", "PE1003")), "PE1003", 1),
        Profile(8, mutable.MutableList(KeyValue("p_id", "PB2004")), "PB2004", 1),
        Profile(9, mutable.MutableList(KeyValue("p_id", "PA1005")), "PA1005", 1),
        Profile(10, mutable.MutableList(KeyValue("p_id", "PU1006")), "PU1006", 1),
        Profile(11, mutable.MutableList(KeyValue("p_id", "PA1007")), "PA1007", 1),
        Profile(12, mutable.MutableList(KeyValue("p_id", "PG10091")), "PG10091", 1),
        Profile(13, mutable.MutableList(KeyValue("p_id", "PG10101")), "PG10101", 1),
        Profile(14, mutable.MutableList(KeyValue("p_id", "PG10102")), "PG10102", 1))
    )
    val (columnNames, rows) = renderResultWithPreloadProfiles(
      dataSet1, dataSet2, secondEPStartID,
      profiles, matchedPairs,
      showSimilarity, profiles1, profiles2
    )
    assertResult(Seq("Similarity", "P1-ID", "P1-t_pid", "P2-ID"))(columnNames)
    CollectionsComparision.assertSameIgnoreOrder(List(
      List("1.0", "TCN001312", "PG10091", "PG10091"),
      List("1.0", "TCN001278", "U1001", "PU1001")
    ), rows.collect.map(x => x.toSeq.toList).toList)
  }

}
