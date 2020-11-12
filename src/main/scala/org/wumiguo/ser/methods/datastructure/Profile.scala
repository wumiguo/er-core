package org.wumiguo.ser.methods.datastructure

/**
 * Represent a profile with it's attributes, sourceId
 * e.g. Profile(123,[("title","helloworld"),("publishDate","20200722")],"P0123",1)
 */
/**
 * @author levinliu
 *         Created on 2020/6/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
case class Profile(id: Int,
                   attributes: scala.collection.mutable.MutableList[KeyValue] = new scala.collection.mutable.MutableList(),
                   originalID: String = "",
                   sourceId: Int = 0) extends ProfileTrait with Serializable {

  /**
   * Add an attribute to the list of attributes
   *
   * @param a attribute to add
   **/
  def addAttribute(a: KeyValue): Unit = {
    attributes += a
  }

  override def toString: String = {
    "Profile(" + id + "," + attributes + ", \"" + originalID + "\"," + sourceId + ")"
  }
}
