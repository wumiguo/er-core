package org.wumiguo.ser.dataloader

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.common.{SparkEnvSetup, SparkTestingEnvSetup}
import org.wumiguo.ser.dataloader.filter.SpecificFieldValueFilter
import org.wumiguo.ser.flow.configuration.DataSetConfiguration
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}
import org.wumiguo.ser.testutil.TestDirs

import scala.collection.mutable

/**
 * @author levinliu
 *         Created on 2020/11/8
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class ProfileLoaderFactoryTest extends AnyFlatSpec with SparkTestingEnvSetup {


  it should "use csv profile loader to load" in {
    val path = TestDirs.resolveTestResourcePath("data/csv/dt01.min.csv")
    val loader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(path))
    assert(loader.isInstanceOf[CSVProfileLoader.type])
  }

  it should "use parquet profile loader to load" in {
    val path = TestDirs.resolveTestResourcePath("data/parquet/dt01.parquet")
    val loader = ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(path))
    assert(loader.isInstanceOf[ParquetProfileLoader.type])
  }

}
