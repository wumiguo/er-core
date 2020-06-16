package org.wumiguo.ser.methods.datastructure

/**
  *
  * Edge trait
  *
  * @author Song Zhu
  * @since 2016-12-14
  */
trait EdgeTrait {
  /* First profile ID */
  val firstProfileID : Int
  /** Second profile ID */
  val secondProfileID : Int
  /** Return the equivalent entity match of this edge
    * @param map a map that maps the interlan id to the groundtruth id
    * */
  def getEntityMatch(map: Map[Int, String]): MatchingEntities = MatchingEntities(map(firstProfileID),map(secondProfileID))

}
