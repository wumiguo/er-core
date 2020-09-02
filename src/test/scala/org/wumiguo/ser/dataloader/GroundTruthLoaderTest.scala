package org.wumiguo.ser.dataloader

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.MatchingEntities
import org.wumiguo.ser.testutil.TestDirs

/**
 * @author levinliu
 *         Created on 2020/9/1
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class GroundTruthLoaderTest extends AnyFlatSpec with SparkEnvSetup{
  val spark = createLocalSparkSession(this.getClass.getName)

  it should "load ground truth with header" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-withheader.csv")
    val meRdd = GroundTruthLoader.loadGroundTruth(gtPath, ",", true)
    assert(meRdd != null)
    assert(meRdd.count() == 200)
    val first = meRdd.sortBy(_.firstEntityID, ascending = true).first()
    assertResult(MatchingEntities("1", "1093"))(first)
  }

  it should "load ground truth with header and take header as normal entry" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-withheader.csv")
    val meRdd = GroundTruthLoader.loadGroundTruth(gtPath, ",", false)
    assert(meRdd != null)
    assert(meRdd.count() == 201)
    val first = meRdd.sortBy(_.firstEntityID, ascending = false).first()
    assertResult(MatchingEntities("entityId1", "entityId2"))(first)
  }

  it should "load valid ground truth" in {
    val gtPath = TestDirs.resolveTestResourcePath("data/csv/dblpAcmIdDuplicates-noheader.gen.csv")
    val meRdd = GroundTruthLoader.loadGroundTruth(gtPath)
    assert(meRdd != null)
    assert(meRdd.count() == 3)
    meRdd.foreach(e => println(e))
    val first = meRdd.first()
    assertResult(MatchingEntities("1821", "1345"))(first)
  }


}
