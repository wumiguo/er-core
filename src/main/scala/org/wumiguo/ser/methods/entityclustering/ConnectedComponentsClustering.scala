package org.wumiguo.ser.methods.entityclustering

import org.apache.spark.rdd.RDD
import EntityClusterUtils.{addUnclusteredProfiles, connectedComponents}
import org.wumiguo.ser.methods.datastructure.{Profile, WeightedEdge}

/**
 * @author levinliu
 *         Created on 2020/6/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object ConnectedComponentsClustering extends EntityClusteringTrait {

  override def getClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge],
                           maxProfileID: Int, edgesThreshold: Double = 0,
                           separatorID: Int = -1): RDD[(Int, Set[Int])] = {
    val cc = connectedComponents(edges.filter(_.weight >= edgesThreshold))
    val a = cc.map(x => x.flatMap(y => y._1 :: y._2 :: Nil)).zipWithIndex().map(x => (x._2.toInt, x._1.toSet))
    addUnclusteredProfiles(profiles, a)
  }

  def getWeightedClusters(profiles: RDD[Profile], edges: RDD[WeightedEdge],
                          maxProfileID: Int, edgesThreshold: Double = 0,
                          separatorID: Int = -1): RDD[(Int, Set[Int])] = {
    val cc = connectedComponents(edges.filter(_.weight >= edgesThreshold))
    val a = cc.map(x => x.flatMap(y => y._1 :: y._2 :: Nil)).zipWithIndex().map(x => (x._2.toInt, x._1.toSet))
    addUnclusteredProfiles(profiles, a)
  }
}
