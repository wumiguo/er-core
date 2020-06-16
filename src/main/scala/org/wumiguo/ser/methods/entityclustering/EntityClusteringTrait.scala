package org.wumiguo.ser.methods.entityclustering

import org.apache.spark.rdd.RDD
import org.wumiguo.ser.methods.datastructure.{Profile, WeightedEdge}

/**
 * @author levinliu
 * Created on 2020/6/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
trait EntityClusteringTrait {

  def getClusters(profiles: RDD[Profile],
                  edges: RDD[WeightedEdge],
                  maxProfileID: Int,
                  edgesThreshold: Double = 0,
                  separatorID: Int = -1
                 ): RDD[(Int, Set[Int])]
}
