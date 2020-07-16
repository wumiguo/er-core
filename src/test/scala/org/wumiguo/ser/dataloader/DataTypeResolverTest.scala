package org.wumiguo.ser.dataloader

import DataType._
import org.scalatest.FlatSpec

/**
 * @author levinliu
 *         Created on 2020/7/16
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class DataTypeResolverTest extends FlatSpec {
  it should "resolve data type " in {
    assertResult(CSV)(DataTypeResolver.getDataType("a.csv"))
    assertResult(CSV)(DataTypeResolver.getDataType("a.CSV"))
    assertResult(JSON)(DataTypeResolver.getDataType("a.json"))
    assertResult(PARQUET)(DataTypeResolver.getDataType("a.parquet"))
    var hasErr = false
    try {
      assertResult(PARQUET)(DataTypeResolver.getDataType("a.java"))
    } catch {
      case e: RuntimeException =>
        hasErr = true
        assert(e.getMessage == "Out of support data type DataType.ValueSet(CSV, JSON, PARQUET)")
    }
    assert(hasErr)
  }
}
