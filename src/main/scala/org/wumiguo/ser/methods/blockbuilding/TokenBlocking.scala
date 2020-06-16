package org.wumiguo.ser.methods.blockbuilding

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.rdd.RDD
import org.wumiguo.ser.methods.blockbuilding.LSH.Settings
import org.wumiguo.ser.methods.datastructure
import org.wumiguo.ser.methods.datastructure.{BlockAbstract, BlockClean, BlockDirty, KeyValue, KeysCluster, Profile}

/**
  * Implements the token blocking
  *
  * @author Luca Gagliardelli
  * @since 2016/12/07
  */
object TokenBlocking {

  def removeBadWords(input: RDD[(String, Int)]): RDD[(String, Int)] = {
    val sc = SparkContext.getOrCreate()
    val stopwords = sc.broadcast(StopWordsRemover.loadDefaultStopWords("english"))
    input
      .filter(x => x._1.matches("[A-Za-z]+") || x._1.matches("[0-9]+"))
      .filter(x => !stopwords.value.contains(x._1))
  }


  /**
    * Performs the token blocking
    *
    * @param profiles      input to profiles to create blocks
    * @param separatorIDs  list of the ids that separates the different data sources (in case of Clean-Clean ER), "-1" if Dirty ER
    * @param keysToExclude keys to exclude from the blocking process
    * @return the blocks
    */
  def createBlocks(profiles: RDD[Profile],
                   separatorIDs: Array[Int] = Array.emptyIntArray,
                   keysToExclude: Iterable[String] = Nil,
                   removeStopWords: Boolean = false,
                   createKeysFunctions: (Iterable[KeyValue], Iterable[String]) => Iterable[String] = BlockingKeysStrategies.createKeysFromProfileAttributes): RDD[BlockAbstract] = {
    /* For each profile returns the list of his tokens, produces (profileID, [list of tokens]) */
    val tokensPerProfile = profiles.map(profile => (profile.id, createKeysFunctions(profile.attributes, keysToExclude).filter(_.trim.length > 0).toSet))
    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    //val profilePerKey = tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey().filter(_._2.size > 1)
    val a = if (removeStopWords) {
      removeBadWords(tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID))
    } else {
      tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID)
    }
    val profilePerKey = a.groupByKey().filter(_._2.size > 1)

    /* For each token divides the profiles in two lists according to the datasets they come from (only for Clean-Clean) */
    val profilesGrouped = profilePerKey.map {
      c =>
        val entityIds = c._2.toSet
        val blockEntities = if (separatorIDs.isEmpty) Array(entityIds) else TokenBlocking.separateProfiles(entityIds, separatorIDs)
        blockEntities
    }

    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      block =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1

    } zipWithIndex()

    /* Map each row in an object BlockClean or BlockDirty */
    profilesGroupedWithIds map {
      case (entityIds, blockId) =>
        if (separatorIDs.isEmpty)
          BlockDirty(blockId.toInt, entityIds)
        else BlockClean(blockId.toInt, entityIds)
    }
  }

  /**
    *
    * @param profiles
    * @param separatorIDs
    * @param clusters
    * @param keysToExclude
    * @param excludeDefaultCluster
    * @param clusterNameSeparator
    * @return
    */
  def createBlocksClusterDebug(profiles: RDD[Profile],
                               separatorIDs: Array[Int],
                               clusters: List[KeysCluster],
                               keysToExclude: Iterable[String] = Nil,
                               excludeDefaultCluster: Boolean = false,
                               clusterNameSeparator: String = Settings.SOURCE_NAME_SEPARATOR):
  (RDD[BlockAbstract], scala.collection.Map[String, Map[Int, Iterable[String]]]) = {

    /** Obtains the ID of the default cluster: all the elements that are not in other clusters finish in this one */
    val defaultClusterID = clusters.maxBy(_.id).id
    /** Creates a map that contains the entropy for each cluster */
    val entropies = clusters.map(cluster => (cluster.id, cluster.entropy)).toMap
    /** Creates a map that map each key of a cluster to it id */
    val clusterMap = clusters.flatMap(c => c.keys.map(key => (key, c.id))).toMap

    /* Generates the tokens for each profile */
    val tokensPerProfile1 = profiles.map {
      profile =>
        /* Calculates the dataset to which this token belongs */
        val dataset = profile.sourceId + clusterNameSeparator

        /* Generates the tokens for this profile */
        val tokens = profile.attributes.flatMap {
          keyValue =>
            if (keysToExclude.exists(_.equals(keyValue.key))) {
              //If this key is in the exclusion list ignores this tokens
              Nil
            }
            else {
              val key = dataset + keyValue.key //Add the dataset suffix to the key
              val clusterID = clusterMap.getOrElse(key, defaultClusterID) //Gets the id of this key cluster, if it not exists returns the default one
              val values = keyValue.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).map(_.toLowerCase.trim).filter(_.trim.nonEmpty).distinct //Split the values and obtain the tokens
              values.map(_ + "_" + clusterID).map(x => (x, key)) //Add the cluster id to the tokens
            }
        }.filter(x => x._1.nonEmpty)

        (profile.id, tokens)
    }

    val debug = tokensPerProfile1.flatMap { case (profileId, tokens) =>
      tokens.map { case (token, attribute) =>
        (token, (profileId, attribute))
      }
    }.groupByKey().map(x => (x._1, x._2.groupBy(_._1).map(y => (y._1, y._2.map(_._2))))).collectAsMap()

    val tokensPerProfile = tokensPerProfile1.map(x => (x._1, x._2.map(_._1).distinct))

    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = {
      if (excludeDefaultCluster) {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey().filter(!_._1.endsWith("_" + defaultClusterID))
      }
      else {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()
      }
    }

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = profilePerKey map {
      case (blockingKey, entityIds) =>
        val blockEntities = {
          if (separatorIDs.isEmpty) {
            Array(entityIds.toSet)
          }
          else {
            TokenBlocking.separateProfiles(entityIds.toSet, separatorIDs)
          }
        }
        var clusterID = defaultClusterID
        val entropy = {
          try {
            clusterID = blockingKey.split("_").last.toInt
            val e = entropies.get(clusterID)
            if (e.isDefined) e.get else 0.0
          }
          catch {
            case _: Throwable => 0.0
          }
        }
        (blockEntities, entropy, clusterID, blockingKey)
    }

    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      case (block, entropy, clusterID, blockingKey) =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1
    } zipWithIndex()

    /* Map each row in an object BlockClean or BlockDirty */
    val blocks: RDD[BlockAbstract] = profilesGroupedWithIds map {
      case ((entityIds, entropy, clusterID, blockingKey), blockId) =>
        if (separatorIDs.isEmpty) datastructure.BlockDirty(blockId.toInt, entityIds, entropy, clusterID, blockingKey = blockingKey)
        else datastructure.BlockClean(blockId.toInt, entityIds, entropy, clusterID, blockingKey = blockingKey)
    }

    (blocks, debug)
  }

  /**
    * Performs the token blocking clustering the attributes by the keys.
    *
    * @param profiles      input to profiles to create blocks
    * @param separatorIDs  id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param clusters
    * @param keysToExclude keys to exclude from the blocking process
    * @return the blocks
    */
  def createBlocksCluster(profiles: RDD[Profile],
                          separatorIDs: Array[Int],
                          clusters: List[KeysCluster],
                          keysToExclude: Iterable[String] = Nil,
                          excludeDefaultCluster: Boolean = false,
                          clusterNameSeparator: String = Settings.SOURCE_NAME_SEPARATOR): RDD[BlockAbstract] = {

    /** Obtains the ID of the default cluster: all the elements that are not in other clusters finish in this one */
    val defaultClusterID = clusters.maxBy(_.id).id
    /** Creates a map that contains the entropy for each cluster */
    val entropies = clusters.map(cluster => (cluster.id, cluster.entropy)).toMap
    /** Creates a map that map each key of a cluster to it id */
    val clusterMap = clusters.flatMap(c => c.keys.map(key => (key, c.id))).toMap

    /* Generates the tokens for each profile */
    val tokensPerProfile = profiles.map {
      profile =>
        /* Calculates the dataset to which this token belongs */
        val dataset = profile.sourceId + clusterNameSeparator

        /* Generates the tokens for this profile */
        val tokens = profile.attributes.flatMap {
          keyValue =>
            if (keysToExclude.exists(_.equals(keyValue.key))) {
              //If this key is in the exclusion list ignores this tokens
              Nil
            }
            else {
              val key = dataset + keyValue.key //Add the dataset suffix to the key
              val clusterID = clusterMap.getOrElse(key, defaultClusterID) //Gets the id of this key cluster, if it not exists returns the default one
              val values = keyValue.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).map(_.toLowerCase.trim).filter(_.trim.nonEmpty).distinct //Split the values and obtain the tokens
              values.map(_ + "_" + clusterID) //Add the cluster id to the tokens
            }
        }.filter(_.nonEmpty)

        (profile.id, tokens.distinct)
    }

    /* Associate each profile to each token, produces (tokenID, [list of profileID]) */
    val profilePerKey = {
      if (excludeDefaultCluster) {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey().filter(!_._1.endsWith("_" + defaultClusterID))
      }
      else {
        tokensPerProfile.flatMap(BlockingUtils.associateKeysToProfileID).groupByKey()
      }
    }

    /* For each tokens divides the profiles in two lists according to the original datasets where they come (in case of Clean-Clean) */
    val profilesGrouped = profilePerKey map {
      case (blockingKey, entityIds) =>
        val blockEntities = {
          if (separatorIDs.isEmpty) {
            Array(entityIds.toSet)
          }
          else {
            TokenBlocking.separateProfiles(entityIds.toSet, separatorIDs)
          }
        }
        var clusterID = defaultClusterID
        val entropy = {
          try {
            clusterID = blockingKey.split("_").last.toInt
            val e = entropies.get(clusterID)
            if (e.isDefined) {
              e.get
            }
            else {
              0.0
            }
          }
          catch {
            case _: Throwable => 0.0
          }
        }
        (blockEntities, entropy, clusterID, blockingKey)
    }

    /* Removes blocks that contains only one profile, and associate an unique ID to each block */
    val profilesGroupedWithIds = profilesGrouped filter {
      case (block, entropy, clusterID, blockingKey) =>
        if (separatorIDs.isEmpty) block.head.size > 1
        else block.count(_.nonEmpty) > 1
    } zipWithIndex()

    /* Map each row in an object BlockClean or BlockDirty */
    profilesGroupedWithIds map {
      case ((entityIds, entropy, clusterID, blockingKey), blockId) =>
        if (separatorIDs.isEmpty) datastructure.BlockDirty(blockId.toInt, entityIds, entropy, clusterID, blockingKey = blockingKey)
        else datastructure.BlockClean(blockId.toInt, entityIds, entropy, clusterID, blockingKey = blockingKey)
    }
  }

  /**
    *
    * @param elements
    * @param separators
    * @return
    */
  def separateProfiles(elements: Set[Int], separators: Array[Int]): Array[Set[Int]] = {
    var input = elements
    var output: List[Set[Int]] = Nil
    separators.foreach { sep =>
      val a = input.partition(_ <= sep)
      input = a._2
      output = a._1 :: output
    }
    output = input :: output

    output.reverse.toArray
  }
}
