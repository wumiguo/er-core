package org.wumiguo.ser.dataloader

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.dataloader.filter.SpecificFieldValueFilter
import org.wumiguo.ser.methods.datastructure.{KeyValue, MatchingEntities, Profile}
import org.wumiguo.ser.testutil.TestDirs

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/6/19
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class CSVProfileLoaderTest extends AnyFlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(this.getClass.getName)

  it should "load ground truth with header" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-withheader.csv")
    val meRdd = CSVProfileLoader.loadGroundTruth(gtPath, ",", true)
    assert(meRdd != null)
    assert(meRdd.count() == 200)
    val first = meRdd.sortBy(_.firstEntityID, ascending = true).first()
    assertResult(MatchingEntities("1", "1093"))(first)
  }

  it should "load ground truth with header and take header as normal entry" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-withheader.csv")
    val meRdd = CSVProfileLoader.loadGroundTruth(gtPath, ",", false)
    assert(meRdd != null)
    assert(meRdd.count() == 201)
    val first = meRdd.sortBy(_.firstEntityID, ascending = false).first()
    assertResult(MatchingEntities("entityId1", "entityId2"))(first)
  }

  it should "load valid ground truth" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-noheader.gen.csv")
    val meRdd = CSVProfileLoader.loadGroundTruth(gtPath)
    assert(meRdd != null)
    assert(meRdd.count() == 3)
    meRdd.foreach(e => println(e))
    val first = meRdd.first()
    assertResult(MatchingEntities("1821", "1345"))(first)
  }

  it should "load 10 entity profiles" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.10.csv")
    val startIdFrom = 100
    val realIDField = ""
    val ep1Rdd = CSVProfileLoader.loadProfiles(ep1Path, startIdFrom, realIDField)
    assert(10 == ep1Rdd.count())
    assertResult(Profile(100, mutable.MutableList(
      KeyValue("_c0", "0"),
      KeyValue("_c1", "The WASA2 object-oriented workflow management system"),
      KeyValue("_c2", "International Conference on Management of Data"),
      KeyValue("_c3", "Gottfried Vossen, Mathias Weske")), "", 0)
    )(ep1Rdd.first())
  }

  it should "load 15 entity profiles with header" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.csv")
    val startIdFrom = 1
    val ep1Rdd = CSVProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, separator = ",", header = true)
    println("ep1Rdd " + ep1Rdd.count())
    ep1Rdd.foreach(x => println("epx " + x))
    assert(15 == ep1Rdd.count())
    assertResult(Profile(1, mutable.MutableList(
      KeyValue("year", "0"),
      KeyValue("title", "The WASA2 object-oriented workflow management system"),
      KeyValue("venue", "International Conference on Management of Data"),
      KeyValue("authors", "Gottfried Vossen, Mathias Weske")), "", 0)
    )(ep1Rdd.first())
  }

  it should "load 15 entity profiles with header and id column" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.csv")
    val startIdFrom = 1
    val ep1Rdd = CSVProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, separator = ",", header = true, realIDField = "year")
    println("ep1Rdd " + ep1Rdd.count())
    ep1Rdd.foreach(x => println("epx " + x))
    assert(15 == ep1Rdd.count())
    assertResult(Profile(1, mutable.MutableList(
      KeyValue("title", "The WASA2 object-oriented workflow management system"),
      KeyValue("venue", "International Conference on Management of Data"),
      KeyValue("authors", "Gottfried Vossen, Mathias Weske")), "0", 0)
    )(ep1Rdd.first())
  }

  it should "load with filter type1" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.v2.csv")
    val startIdFrom = 1
    val fieldValuesScope = List(KeyValue("year", "2010"), KeyValue("year", "2018"))
    val ep1Rdd = CSVProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, separator = ",", header = true, realIDField = "year",
      fieldValuesScope = fieldValuesScope, filter = SpecificFieldValueFilter)
    println("ep1Rdd " + ep1Rdd.count())
    ep1Rdd.foreach(x => println("epx " + x))
    assert(7 == ep1Rdd.count())
    assertResult(Profile(1, mutable.MutableList(
      KeyValue("title", "The WASA2 object-oriented workflow management system"),
      KeyValue("venue", "International Conference on Management of Data"),
      KeyValue("authors", "Gottfried Vossen, Mathias Weske")), "2010", 0)
    )(ep1Rdd.first())
  }

  it should "load with filter type2" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.v2.csv")
    val startIdFrom = 1
    val fieldValuesScope = List(KeyValue("year", "2010"), KeyValue("authors", "Gottfried Vossen, Mathias Weske"))
    val ep1Rdd = CSVProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, separator = ",", header = true, realIDField = "year",
      fieldValuesScope = fieldValuesScope, filter = SpecificFieldValueFilter)
    println("ep1Rdd " + ep1Rdd.count())
    ep1Rdd.foreach(x => println("epx " + x))
    assert(1 == ep1Rdd.count())
    assertResult(Profile(1, mutable.MutableList(
      KeyValue("title", "The WASA2 object-oriented workflow management system"),
      KeyValue("venue", "International Conference on Management of Data"),
      KeyValue("authors", "Gottfried Vossen, Mathias Weske")), "2010", 0)
    )(ep1Rdd.first())
  }

  it should "load with filter type3" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.v2.csv")
    val startIdFrom = 1
    val fieldValuesScope = List(KeyValue("year", "2010"), KeyValue("year", "2018"),
      KeyValue("authors", "Gottfried Vossen, Mathias Weske"),KeyValue("authors","Bart Meltzer, Robert Glushko"))
    val ep1Rdd = CSVProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, separator = ",", header = true, realIDField = "year",
      fieldValuesScope = fieldValuesScope, filter = SpecificFieldValueFilter)
    println("ep1Rdd " + ep1Rdd.count())
    ep1Rdd.foreach(x => println("epx " + x))
    assert(2 == ep1Rdd.count())
    assertResult(Profile(1, mutable.MutableList(
      KeyValue("title", "The WASA2 object-oriented workflow management system"),
      KeyValue("venue", "International Conference on Management of Data"),
      KeyValue("authors", "Gottfried Vossen, Mathias Weske")), "2010", 0)
    )(ep1Rdd.first())
  }

  it should "load with filter type4" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.h.15.v2.csv")
    val startIdFrom = 1
    val fieldValuesScope = List()
    val ep1Rdd = CSVProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, separator = ",", header = true, realIDField = "year",
      fieldValuesScope = fieldValuesScope, filter = SpecificFieldValueFilter)
    println("ep1Rdd " + ep1Rdd.count())
    ep1Rdd.foreach(x => println("epx " + x))
    assert(15 == ep1Rdd.count())
    assertResult(Profile(1, mutable.MutableList(
      KeyValue("title", "The WASA2 object-oriented workflow management system"),
      KeyValue("venue", "International Conference on Management of Data"),
      KeyValue("authors", "Gottfried Vossen, Mathias Weske")), "2010", 0)
    )(ep1Rdd.first())
  }
}
