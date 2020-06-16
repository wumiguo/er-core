package org.wumiguo.ser.methods.datastructure

/**
 * @author levinliu
 * Created on 2020/6/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
case class ProfilePBlocks(profileID : Long, profile : Profile, blocks : Set[BlockWithComparisonSize]) extends Serializable{}
