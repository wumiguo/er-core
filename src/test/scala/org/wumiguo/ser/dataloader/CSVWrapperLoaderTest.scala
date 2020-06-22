package org.wumiguo.ser.dataloader

import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.testutil.TestDirs

/**
 * @author levinliu
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class CSVWrapperLoaderTest extends FlatSpec with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should " be able to load good entity profiles csv" in {
    val testCsv = TestDirs.resolveDataPath("csv/sampleEP1.csv")
    val startIDFrom = 0
    val realIDField = "entityId1"
    val rdd = CSVWrapper.loadProfiles(testCsv, startIDFrom, realIDField)
    assert(rdd != null)
  }
}
