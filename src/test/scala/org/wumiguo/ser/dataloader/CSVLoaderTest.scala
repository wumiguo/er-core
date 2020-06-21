package org.wumiguo.ser.dataloader

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.MatchingEntities
import org.wumiguo.ser.testutil.TestDirs

/**
 * @author levinliu
 *         Created on 2020/6/19
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class CSVLoaderTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(this.getClass.getName)

  it should "load ground truth with header" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-withheader.csv")
    val meRdd = CSVLoader.loadGroundTruth(gtPath, ",", true)
    assert(meRdd != null)
    assert(meRdd.count() == 200)
    val first = meRdd.sortBy(_.firstEntityID, ascending = true).first()
    assertResult(MatchingEntities("1", "1093"))(first)
  }

  it should "load ground truth with header and take header as normal entry" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-withheader.csv")
    val meRdd = CSVLoader.loadGroundTruth(gtPath, ",", false)
    assert(meRdd != null)
    assert(meRdd.count() == 201)
    val first = meRdd.sortBy(_.firstEntityID, ascending = false).first()
    assertResult(MatchingEntities("entityId1", "entityId2"))(first)
  }

  it should "load valid ground truth" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-noheader.gen.csv")
    val meRdd = CSVLoader.loadGroundTruth(gtPath)
    assert(meRdd != null)
    assert(meRdd.count() == 3)
    meRdd.foreach(e => println(e))
    val first = meRdd.first()
    assertResult(MatchingEntities("1821", "1345"))(first)
  }

  it should "load entity profiles" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.gen.csv")
    val startIdFrom = 0
    val realIDField = ""
    val ep1Rdd = CSVLoader.loadProfiles(ep1Path, startIdFrom, realIDField)
    println("ep1Rdd " + ep1Rdd.count())
    ep1Rdd.foreach(x => println("epx "+x))
  }
  it should "load entity profiles from 2nd row" in {
    val ep1Path = TestDirs.resolveTestResourcePath("data/csv/acmProfiles.gen.csv")
    val startIdFrom = 1
    val realIDField = ""
    val ep1Rdd = CSVLoader.loadProfiles(ep1Path, startIdFrom, realIDField)
    println("ep1Rdd " + ep1Rdd.count())
    ep1Rdd.foreach(x => println("epx "+x))
  }
}
