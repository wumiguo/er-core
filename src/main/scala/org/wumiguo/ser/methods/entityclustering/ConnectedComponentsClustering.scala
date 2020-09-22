package org.wumiguo.ser.methods.entityclustering

import org.apache.spark.rdd.RDD
import EntityClusterUtils.{addUnclusteredProfiles, connectedComponents}
import org.wumiguo.ser.methods.datastructure.{Profile, WeightedEdge}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
                          separatorID: Int = -1): RDD[(Int, Iterable[(Int, Map[(Int, Int), Double])])] = {
    val cc = connectedComponents(edges.filter(_.weight >= edgesThreshold))
    val a = cc.map(x => x.flatMap(y => (y._1, Map(y._1 -> y._2 -> y._3)) :: (y._2, Map(y._1 -> y._2 -> y._3)) :: Nil)).zipWithIndex().map(x => (x._2.toInt, x._1))
    //addUnclusteredProfiles(profiles, a)
    a
  }

  def getWeightedClustersV2(profiles: RDD[Profile], edges: RDD[WeightedEdge],
                            maxProfileID: Int, edgesThreshold: Double = 0,
                            separatorID: Int = -1): RDD[(Int, (Set[Int], Map[(Int, Int), Double]))] = {
    val connectedComp = connectedComponents(edges.filter(_.weight >= edgesThreshold))
    val pairs = connectedComp
      .map(x => x.flatMap(y => (y._1, Map(y._1 -> y._2 -> y._3)) :: (y._2, Map(y._1 -> y._2 -> y._3)) :: Nil))
      .zipWithIndex().map(x => (x._2.toInt, x._1))
    pairs.map(x => (x._1, (x._2.map(_._1).toSet, x._2.map(_._2).reduce((x, y) => x ++ y))))
  }

  def linkWeightedCluster(profiles: RDD[Profile], edges: RDD[WeightedEdge],
                          maxProfileID: Int, edgesThreshold: Double = 0,
                          separatorID: Int = -1): //Unit
  RDD[(Int, (Set[Int], Map[(Int, Int), Double]))]
  = {
    val clustersWithWeights = getWeightedClustersV2(profiles, edges, maxProfileID, edgesThreshold, separatorID)
    clustersWithWeights.map(x => {
      val y = x._2
      (x._1, (y._1, {
        val ids = y._1.toArray
        var pairs = new ArrayBuffer[(Int, Int)]()
        for (i <- 0 until ids.size - 1) {
          for (j <- i + 1 until ids.size) {
            pairs :+= (ids(i), ids(j))
          }
        }
        //TODO: use graph algo to calculate the path
        pairs.filter(p => p._1 != p._2).map(p => Map(p -> y._2.getOrElse(p, y._2.getOrElse(p.swap, 0.00001)))).reduce(_ ++ _)
      })
      )
    })
  }
}
