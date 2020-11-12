package org.wumiguo.ser.common

import org.scalatest.flatspec.AnyFlatSpec
import org.wumiguo.ser.methods.datastructure.Profile

/**
 * @author levinliu
 *         Created on 2020/11/12
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object ProfilesListComparison extends AnyFlatSpec {
  /**
   * compare the given 2 profiles list without any concern on ordering
   *
   * @param expectProfiles
   * @param actualProfiles
   * @return
   */
  def sameIgnoreOrder(expectProfiles: List[Profile], actualProfiles: List[Profile]): Boolean = {
    if (expectProfiles.size == actualProfiles.size) {
      var counter = 0
      for (p1 <- expectProfiles) {
        for (p2 <- actualProfiles) {
          if (p1 == p2) {
            counter = counter + 1
          }
        }
      }
      val result = counter == expectProfiles.size
      if (!result) {
        assertResult(expectProfiles)(actualProfiles)
      }
      result
    } else {
      assertResult(expectProfiles)(actualProfiles)
      false
    }
  }

}
