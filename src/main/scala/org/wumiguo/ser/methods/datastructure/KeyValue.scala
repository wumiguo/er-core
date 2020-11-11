package org.wumiguo.ser.methods.datastructure

/**
 * represent a attribute (key=attributeName,value=attributeValue)
 *
 * @param key
 * @param value
 */
case class KeyValue(key: String, value: String) extends Serializable {
  override def toString: String = {
    "KeyValue(\"" + key + "\", \"" + value + "\")"
  }
}
