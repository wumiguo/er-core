package org.wumiguo.ser.methods.datastructure

/**
 * ProfileBlocks : represent a profile with underlying blocks with comparisionSize.
 * E.g. a Profile with clean blocks
 * (pId=1,
 *  blocks=Set(
 *   BlockClean(1, Array[Set[Int]](Set[Int](3, 2),Set[Int](2, 5)), 0.9, 1, "tech_222")
 *   BlockClean(2, Array[Set[Int]](Set[Int](3, 1),Set[Int](1)), 0.9, 1, "bigdata_222")
 * )
 * and its profileBLocks should be:
 *  ProfileBlocks(1,Set(BlockWithComparisonSize(1,4.0),BlockWithComparisonSize(2,2.0)))
 *
 * @author levinliu
 * @param profileID
 * @param blocks
 */
case class ProfileBlocks(profileID: Int, blocks: Set[BlockWithComparisonSize]) extends Serializable {}
