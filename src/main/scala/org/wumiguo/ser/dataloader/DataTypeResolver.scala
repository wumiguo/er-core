package org.wumiguo.ser.dataloader

import org.wumiguo.ser.dataloader.DataType.DataType

/**
 * @author levinliu
 *         Created on 2020/7/16
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object DataTypeResolver {

  def getDataType(dataFile: String): DataType = {
    import DataType._
    val theDataFile = dataFile.toLowerCase()
    if (theDataFile.endsWith(".csv")) {
      CSV
    } else if (theDataFile.endsWith(".json")) {
      JSON
    } else if (theDataFile.endsWith(".parquet")) {
      PARQUET
    } else throw new RuntimeException("Out of support data type " + DataType.values)
  }

}
