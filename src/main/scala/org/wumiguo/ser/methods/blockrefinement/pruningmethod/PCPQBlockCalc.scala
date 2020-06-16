package org.wumiguo.ser.methods.blockrefinement.pruningmethod

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.wumiguo.ser.methods.datastructure.{BlockAbstract, ProfileBlocks, UnweightedEdge}
import org.wumiguo.ser.methods.util.Converters

/**
  * Created by Luca on 03/05/2017.
  */
object PCPQBlockCalc {


  def getPcPq2(blocks: RDD[BlockAbstract], newGT: Set[(Int, Int)], maxProfileID: Int, separatorID: Array[Int]): (Float, Float, Double, Long) = {

    val sc = SparkContext.getOrCreate()

    val blockIndexMap1 = blocks.map(b => (b.blockID, b.profiles)).collectAsMap()
    val blockIndex1 = sc.broadcast(blockIndexMap1)
    val gt1 = sc.broadcast(newGT)
    val profileBlocks1 = Converters.blocksToProfileBlocks(blocks)

    val edgesAndCount = getStats(
      profileBlocks1,
      blockIndex1,
      maxProfileID,
      separatorID,
      gt1
    )

    val numEdges = edgesAndCount.map(_._1).sum()
    val edges = edgesAndCount.flatMap(_._2).distinct()
    val perfectMatch = edges.count()
    val newGTSize = newGT.size

    val pc = {
      try {
        perfectMatch.toFloat / newGTSize.toFloat
      }
      catch {
        case _: Exception => 0
      }
    }
    val pq = {
      try {
        perfectMatch.toFloat / numEdges.toFloat
      }
      catch {
        case _: Exception => 0
      }
    }
    gt1.unpersist()

    (pc, pq, numEdges, perfectMatch)
  }

  /**
    * Computes PC and PQ on a block collection
    **/
  def getPcPq(blocks: RDD[BlockAbstract], newGT: Set[(Int, Int)], maxProfileID: Int, separatorID: Array[Int]): (Double, Double, Double) = {

    val sc = SparkContext.getOrCreate()

    val blockIndexMap1 = blocks.map(b => (b.blockID, b.profiles)).collectAsMap()
    val blockIndex1 = sc.broadcast(blockIndexMap1)
    val gt1 = sc.broadcast(newGT)
    val profileBlocks1 = Converters.blocksToProfileBlocks(blocks)

    val edgesAndCount = getStats(
      profileBlocks1,
      blockIndex1,
      maxProfileID,
      separatorID,
      gt1
    )

    val numEdges = edgesAndCount.map(_._1).sum()
    val edges = edgesAndCount.flatMap(_._2).distinct()
    val perfectMatch = edges.count()
    val newGTSize = newGT.size

    println("Perfect matches " + perfectMatch)
    println("Missed matches " + (newGTSize - perfectMatch))

    val pc = perfectMatch.toFloat / newGTSize.toFloat
    val pq = perfectMatch.toFloat / numEdges.toFloat

    (pc, pq, numEdges)
  }


  /**
    * Computes the Weight Node Pruning
    *
    * @param profileBlocksFiltered profileBlocks after block filtering
    * @param blockIndex            a map that given a block ID returns the ID of the contained profiles
    * @param maxID                 maximum profile ID
    * @param separatorID           maximum profile ID of the first dataset (-1 if there is only one dataset)
    * @param groundtruth           set of true matches
    * @return an RDD that contains for each partition the number of existing edges and the retained edges
    **/
  def getStats(profileBlocksFiltered: RDD[ProfileBlocks],
               blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
               maxID: Int,
               separatorID: Array[Int],
               groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]]
              )
  : RDD[(Double, Iterable[UnweightedEdge])] = {

    pruning(profileBlocksFiltered, blockIndex, maxID, separatorID, groundtruth)
  }

  /**
    * Performs the pruning
    *
    * @param neighbours       an array which contains the IDs of the neighbours
    * @param neighboursNumber number of neighbours
    * @param groundtruth      set of true matches
    * @return a tuple that contains the number of retained edges and the edges that exists in the groundtruth
    **/
  def doPruning(profileID: Int,
                neighbours: Array[Int],
                neighboursNumber: Int,
                groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]]
               ): (Double, Iterable[UnweightedEdge]) = {

    var edges: List[UnweightedEdge] = Nil

    var cont: Double = 0

    for (i <- 0 until neighboursNumber) {
      val neighbourID = neighbours(i)
      cont += 1
      if (groundtruth.value.contains((profileID, neighbourID))) {
        edges = UnweightedEdge(profileID, neighbours(i)) :: edges
      }
    }

    (cont, edges)
  }


  /**
    * Performs the pruning
    *
    * @param profileBlocksFiltered profileBlocks after block filtering
    * @param blockIndex            a map that given a block ID returns the ID of the contained profiles
    * @param maxID                 maximum profile ID
    * @param separatorID           maximum profile ID of the first dataset (-1 if there is only one dataset)
    * @param groundtruth           set of true matches
    * @return an RDD that for each partition contains the number of retained edges and the list of the elements that appears in the groundtruth
    **/
  def pruning(profileBlocksFiltered: RDD[ProfileBlocks],
              blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
              maxID: Int,
              separatorID: Array[Int],
              groundtruth: Broadcast[scala.collection.immutable.Set[(Int, Int)]]
             ): RDD[(Double, Iterable[UnweightedEdge])] = {

    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbours = Array.ofDim[Int](maxID + 1)
      var neighboursNumber = 0

      partition.map { pb =>
        neighboursNumber = calcNeighbors(pb, blockIndex, separatorID, localWeights, neighbours)
        val result = doPruning(pb.profileID, neighbours, neighboursNumber, groundtruth)
        doReset(localWeights, neighbours, neighboursNumber)
        result
      }
    }
  }


  def doReset(weights: Array[Double],
              neighbours: Array[Int],
              neighboursNumber: Int): Unit = {
    for (i <- 0 until neighboursNumber) {
      val neighbourID = neighbours(i)
      weights.update(neighbourID, 0)
    }
  }


  def calcNeighbors(pb: ProfileBlocks,
                    blockIndex: Broadcast[scala.collection.Map[Int, Array[Set[Int]]]],
                    separatorID: Array[Int],
                    weights: Array[Double],
                    neighbours: Array[Int]
                    ): Int = {
    var neighboursNumber = 0
    val profileID = pb.profileID
    val profileBlocks = pb.blocks

    profileBlocks.foreach { block =>
      val blockID = block.blockID
      val blockProfiles = blockIndex.value.get(blockID)

      if (blockProfiles.isDefined) {
        val profilesIDs = {
          if (separatorID.isEmpty) {
            blockProfiles.get.head
          }
          else {
            PruningUtils.getAllNeighbors(profileID, blockProfiles.get, separatorID)
          }
        }

        profilesIDs.foreach { secondProfileID =>
          val neighbourID = secondProfileID.toInt
          if (profileID < neighbourID) {
            weights.update(neighbourID, weights(neighbourID) + 1)

            if (weights(neighbourID) == 1) {
              neighbours.update(neighboursNumber, neighbourID)
              neighboursNumber += 1
            }
          }
        }
      }
    }

    neighboursNumber
  }


}
