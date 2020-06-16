package org.wumiguo.ser.dataloader

import org.scalatest.FlatSpec
import org.wumiguo.ser.testutil.TestDirs

/**
 * @author levinliu
 *         Created on 2020/6/16
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class SerializedLoaderTest extends FlatSpec {
  it should "all good to load good serialized data" in {
    val gtFile = TestDirs.resolveDataPath("/serialized/dblpVsAcmGt-IdDuplicateSet")
    val data = SerializedLoader.loadSerializedGroundtruth(gtFile)
    assert(data != null)
    assert(data.size() == 2)
  }
}
