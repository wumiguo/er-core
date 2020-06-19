package org.wumiguo.ser.methods.datastructure

/**
 * MatchingEntities with 2 entityID to represent the two matching entities
 * @author levinliu
 * @param firstEntityID
 * @param secondEntityID
 */
case class MatchingEntities(firstEntityID: String, secondEntityID: String) {

  /**
   * overwrite equals with comparison on both entityID
   *
   * @param that
   * @return
   */
  override def equals(that: Any): Boolean = {
    that match {
      case that: MatchingEntities =>
        that.firstEntityID == this.firstEntityID && that.secondEntityID == this.secondEntityID
      case _ => false
    }
  }

  /**
   * Use two entityId hashCode concatenation string for comparision
   *
   * @return
   */
  override def hashCode: Int = {
    val firstEntityHashCode = if (firstEntityID == null) 0 else firstEntityID.hashCode
    val secondEntityHashCode = if (secondEntityID == null) 0 else secondEntityID.hashCode
    (firstEntityHashCode + "_" + secondEntityHashCode).hashCode
  }
}
