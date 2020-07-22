package org.wumiguo.ser.methods.datastructure

/**
 * @author levinliu
 * Created on 2020/6/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
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
