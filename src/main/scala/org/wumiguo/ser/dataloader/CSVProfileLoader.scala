package org.wumiguo.ser.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.wumiguo.ser.dataloader.filter.{DummyFieldFilter, FieldFilter}
import org.wumiguo.ser.methods.datastructure
import org.wumiguo.ser.methods.datastructure.{KeyValue, MatchingEntities, Profile}

/**
 * @author levinliu
 *         Created on 2020/6/19
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object CSVProfileLoader extends ProfileLoaderTrait {


  override def load(filePath: String, startIDFrom: Int, realIDField: String, sourceId: Int,
                    fieldsToKeep: List[String], keepRealID: Boolean = false,
                    filter: FieldFilter = DummyFieldFilter,
                    fieldValuesScope: List[KeyValue] = Nil): RDD[Profile] = {
    loadProfilesAdvanceMode(filePath, startIDFrom, realIDField = realIDField,
      sourceId = sourceId, header = true,
      fieldsToKeep = fieldsToKeep, keepRealID = keepRealID,
      filter = filter, fieldValuesScope = fieldValuesScope
    )
  }

  /**
   * Load the profiles from a CSV file.
   **/
  def loadProfiles(filePath: String, startIDFrom: Int, realIDField: String, sourceId: Int = 0): RDD[Profile] = {
    loadProfilesAdvanceMode(filePath, startIDFrom, realIDField = realIDField, sourceId = sourceId)
  }

  /**
   *
   * @param filePath
   * @param startIDFrom
   * @param separator
   * @param header
   * @param explodeInnerFields specify if the inner fields have to be splited.
   *                           *          E.g. if a field contains a list of names separated by comma and you want to split it in single attributes.
   *                           *          The default value is false.
   * @param innerSeparator     if the parameter explodeInnerFields is true, specify the values separator.
   * @param realIDField
   * @param sourceId
   * @param fieldsToKeep
   * @param keepRealID
   * @return
   */
  def loadProfilesAdvanceMode(filePath: String, startIDFrom: Int = 0, separator: String = ",",
                              header: Boolean = false, explodeInnerFields: Boolean = false, innerSeparator: String = ",",
                              realIDField: String = "", sourceId: Int = 0, fieldsToKeep: List[String] = Nil,
                              keepRealID: Boolean = false, filter: FieldFilter = DummyFieldFilter, fieldValuesScope: List[KeyValue] = Nil
                             ): RDD[Profile] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)
    val columnNames = df.columns
    val lcRealIDField = realIDField.toLowerCase
    df.rdd.map(row => rowToAttributes(columnNames, row, explodeInnerFields, innerSeparator))
      .filter(kvList => filter.filter(kvList.toList, fieldValuesScope))
      .zipWithIndex().map {
      profile =>
        val profileID = profile._2.toInt + startIDFrom
        val attributes = profile._1
        val realID = {
          if (realIDField.isEmpty) {
            ""
          }
          else {
            attributes.filter(_.key.toLowerCase() == lcRealIDField).map(_.value).mkString("").trim
          }
        }
        Profile(profileID,
          attributes.filter(kv => {
            val lcKey = kv.key.toLowerCase
            (keepRealID && lcKey == lcRealIDField) ||
              ((lcKey != lcRealIDField) &&
                //if not specify fields to keep then get all field, else get the specific fields
                (fieldsToKeep.isEmpty || fieldsToKeep.contains(kv.key)))
          }),
          realID, sourceId)
    }
  }
}
