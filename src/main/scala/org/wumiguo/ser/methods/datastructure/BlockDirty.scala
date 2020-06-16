package org.wumiguo.ser.methods.datastructure

/**
  * Clean block: the profiles comes from two distinct datasets
  *
  * @author Giovanni Simononi
  * @since 2016/12/07
  */
case class BlockDirty(blockID: Int, profiles: Array[Set[Int]], var entropy: Double = -1, var clusterID: Integer = -1, blockingKey: String = "") extends BlockAbstract with Serializable {
  override def getComparisonSize(): Double = {
    profiles.head.size.toDouble * (profiles.head.size.toDouble - 1)
  }

  override def getComparisons(): Set[(Int, Int)] = {
    profiles.head.toList.combinations(2).map { x =>
      if (x.head < x.last) {
        (x.head, x.last)
      }
      else {
        (x.last, x.head)
      }
    }.toSet
  }
}
