package org.wumiguo.ser.dataloader

import org.wumiguo.ser.model.{EntityProfile, IdDuplicates}

/**
 * No need to implement for now, as we are not going to support serialized object loading
 */
object SerializedLoader extends scala.AnyRef {
  def loadSerializedGroundtruth(fileName: scala.Predef.String): java.util.HashSet[IdDuplicates] = ???

  def loadSerializedDataset(fileName: scala.Predef.String): java.util.ArrayList[EntityProfile] = ???
}
