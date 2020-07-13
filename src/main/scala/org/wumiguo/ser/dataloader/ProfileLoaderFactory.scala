package org.wumiguo.ser.dataloader

object ProfileLoaderFactory {

  val DATA_TYPE_CSV = "CSV"
  val DATA_TYPE_JSON = "JSON"
  val DATA_TYPE_PARQUET = "PARQUET"

  def getDataLoader(dataType: String): ProfileLoaderTrait = {
    dataType match {
      case DATA_TYPE_CSV => CSVProfileLoader
      case DATA_TYPE_JSON => JSONProfileLoader
      case DATA_TYPE_PARQUET => CSVProfileLoader
    }
  }

}
