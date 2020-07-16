package org.wumiguo.ser.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}

import scala.collection.mutable.MutableList

/**
 * @author levinliu
 *         Created on 2020/7/9
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
trait ProfileLoaderTrait {
  /**
   * Load source with fields to keep
   *
   * @param filePath
   * @param startIDFrom
   * @param realIDField
   * @param sourceId
   * @param fieldsToKeep
   * @return Profile Rdd with selected fields
   */
  def load(filePath: String, startIDFrom: Int = 0, realIDField: String, sourceId: Int = 0, fieldsToKeep: List[String] = Nil): RDD[Profile]

  /**
   * Given a row return the list of attributes
   *
   * @param columnNames names of the dataframe columns
   * @param row         single dataframe row
   **/
  def rowToAttributes(columnNames: Array[String], row: Row, explodeInnerFields: Boolean = false, innerSeparator: String = ","): MutableList[KeyValue] = {
    val attributes: MutableList[KeyValue] = new MutableList()
    for (i <- 0 until row.size) {
      try {
        val value = row(i)
        val attributeKey = columnNames(i)

        if (value != null) {
          value match {
            case listOfAttributes: Iterable[Any] =>
              listOfAttributes map {
                attributeValue =>
                  attributes += KeyValue(attributeKey, attributeValue.toString)
              }
            case stringAttribute: String =>
              if (explodeInnerFields) {
                stringAttribute.split(innerSeparator) map {
                  attributeValue =>
                    attributes += KeyValue(attributeKey, attributeValue)
                }
              }
              else {
                attributes += KeyValue(attributeKey, stringAttribute)
              }
            case singleAttribute =>
              attributes += KeyValue(attributeKey, singleAttribute.toString)
          }
        }
      }
      catch {
        case e: Throwable => println(e)
      }
    }
    attributes
  }
}
