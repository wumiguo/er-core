package org.wumiguo.ser.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.methods.datastructure.{MatchingEntities, Profile}
import org.wumiguo.ser.testutil.TestDirs

/**
 * @author levinliu
 *         Created on 2020/6/20
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class WrapperTraitTest extends FlatSpec
  with SparkEnvSetup {
  val spark = createLocalSparkSession(getClass.getName)
  it should "test row to attributes " in {
    val columnNames = Array("staffId", "name", "role", "team", "gender")
    val testDfInputCsv = TestDirs.resolveTestResourcePath("data/csv/sample-dataframe.csv")
    val result = SimpleWrapperTrait.forTest(testDfInputCsv, columnNames)
    result.foreach(x => println("item is = x" + x))
  }

  object SimpleWrapperTrait extends WrapperTrait with Serializable {
    def forTest(testDfInputCsv: String, columnNames: Array[String]) = {
      val sparkSession = SparkSession.builder().getOrCreate()
      val df = sparkSession.read.option("header", true).option("sep", ",").option("delimiter", "\"").csv(testDfInputCsv)
      val result = df.rdd.map(row => rowToAttributes(columnNames, row, false, ""))
      result
    }

    /**
     * Given a file path return an RDD of Profiles
     **/
    override def loadProfiles(filePath: String, startIDFrom: Int, realIDField: String, sourceId: Int): RDD[Profile] = ???

    /**
     * Given a file path return an RDD of EqualEntities
     **/
    override def loadGroundtruth(filePath: String): RDD[MatchingEntities] = ???
  }

}
