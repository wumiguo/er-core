package org.wumiguo.ser.dataloader

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.dataloader.filter.SpecificFieldValueFilter
import org.wumiguo.ser.methods.datastructure.{KeyValue, MatchingEntities, Profile}
import org.wumiguo.ser.testutil.TestDirs

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/8/27
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class ParquetProfileLoaderTest extends AnyFlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(this.getClass.getName)

  it should "load only" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/parquet/dt01.parquet")
    val ep1Rdd = ParquetProfileLoader.load(ep1Path)
    assertResult(12)(ep1Rdd.count())
  }

  it should "load 15 entity profiles" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/parquet/acmProfiles.h.15_v2.parquet")
    val startIdFrom = 100
    val realIDField = ""
    val ep1Rdd = ParquetProfileLoader.load(ep1Path, startIdFrom, realIDField)
    assert(15 == ep1Rdd.count())
    assertResult(Profile(100, mutable.MutableList(
      KeyValue("year", "2010"),
      KeyValue("title", "The WASA2 object-oriented workflow management system"),
      KeyValue("venue", "International Conference on Management of Data"),
      KeyValue("authors", "Gottfried Vossen, Mathias Weske")), "", 0)
    )(ep1Rdd.first())
  }

  it should "load 15 entity profiles with specific fields to keep" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/parquet/acmProfiles.h.15_v2.parquet")
    val startIdFrom = 1
    val ep1Rdd = ParquetProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom,
      fieldsToKeep = List("year", "title"))
    println("ep1Rdd " + ep1Rdd.count())
    ep1Rdd.foreach(x => println("epx " + x))
    assert(15 == ep1Rdd.count())
    assertResult(Profile(1, mutable.MutableList(
      KeyValue("year", "2010"),
      KeyValue("title", "The WASA2 object-oriented workflow management system")))
    )(ep1Rdd.first())
  }

  it should "load with filter type1" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/parquet/acmProfiles.h.15_v2.parquet")
    val startIdFrom = 1
    val fieldValuesScope = List(KeyValue("year", "2010"), KeyValue("year", "2018"))
    val ep1Rdd = ParquetProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, realIDField = "year",
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
    val ep1Path = TestDirs.resolveTestResourcePath("data/parquet/acmProfiles.h.15_v2.parquet")
    val startIdFrom = 1
    val fieldValuesScope = List(KeyValue("year", "2010"), KeyValue("authors", "Gottfried Vossen, Mathias Weske"))
    val ep1Rdd = ParquetProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, realIDField = "year",
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
    val ep1Path = TestDirs.resolveTestResourcePath("data/parquet/acmProfiles.h.15_v2.parquet")
    val startIdFrom = 1
    val fieldValuesScope = List(KeyValue("year", "2010"), KeyValue("year", "2018"),
      KeyValue("authors", "Gottfried Vossen, Mathias Weske"), KeyValue("authors", "Bart Meltzer, Robert Glushko"))
    val ep1Rdd = ParquetProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, realIDField = "year",
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
    val ep1Path = TestDirs.resolveTestResourcePath("data/parquet/acmProfiles.h.15_v2.parquet")
    val startIdFrom = 1
    val fieldValuesScope = List()
    val ep1Rdd = ParquetProfileLoader.loadProfilesAdvanceMode(ep1Path, startIdFrom, realIDField = "year",
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
