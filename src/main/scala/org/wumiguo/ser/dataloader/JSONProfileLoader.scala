package org.wumiguo.ser.dataloader

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json.{JSONArray, JSONObject}
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}

/**
  * Created by Luca on 08/12/2016.
  *
  * JSON Wrapper
  */
object JSONProfileLoader extends ProfileLoaderTrait {


  def parseData(key: String, data: Any, p: Profile, fieldsToKeep: List[String], realIDField: String): Unit = {
    data match {
      case jsonArray: JSONArray =>
        val it = jsonArray.iterator()
        while (it.hasNext) {
          //p.addAttribute(KeyValue(key, it.next().toString))
          parseData(key, it.next(), p, fieldsToKeep, realIDField)
        }
      case jsonbject: JSONObject =>
        val it = jsonbject.keys()
        while (it.hasNext) {
          val key = it.next()
          //p.addAttribute(KeyValue(key, jsonbject.get(key).toString))
          parseData(key, jsonbject.get(key), p, fieldsToKeep, realIDField)
        }
      case _ => p.addAttribute(KeyValue(key, data.toString))
    }
  }

  /**
    * Load the profiles from a JSON file.
    * The JSON must contains a JSONObject for each row, and the JSONObject must be in the form key: value
    * the value can be a single value or an array of values
    **/
  override def load(filePath: String, startIDFrom: Int = 0, realIDField: String = "", sourceId: Int = 0, fieldsToKeep: List[String] = Nil): RDD[Profile] = {
    val sc = SparkContext.getOrCreate()
    val raw = sc.textFile(filePath, sc.defaultParallelism)

    raw.zipWithIndex().map { case (row, id) =>
      val obj = new JSONObject(row)
      val realID = {
        if (realIDField.isEmpty) {
          ""
        }
        else {
          obj.get(realIDField).toString
        }
      }
      val p = Profile(id.toInt + startIDFrom, originalID = realID, sourceId = sourceId)

      val keys = obj.keys()
      while (keys.hasNext) {
        val key = keys.next()
        if (key != realIDField && (fieldsToKeep.isEmpty || fieldsToKeep.contains(key))) {
          val data = obj.get(key)
          parseData(key, data, p, fieldsToKeep, realIDField)
        }
      }
      p
    }
  }


}
