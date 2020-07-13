package org.wumiguo.ser.dataloader

import org.apache.spark.rdd.RDD
import org.wumiguo.ser.methods.datastructure.Profile

/**
 * @author levinliu
 *         Created on 2020/7/9
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
trait ProfileLoaderTrait {
  def load : String=> RDD[Profile]
}
