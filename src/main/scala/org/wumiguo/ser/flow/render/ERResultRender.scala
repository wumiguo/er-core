package org.wumiguo.ser.flow.render

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}
import org.slf4j.LoggerFactory
import org.wumiguo.ser.dataloader.{DataTypeResolver, ProfileLoaderFactory, ProfileLoaderTrait}
import org.wumiguo.ser.dataloader.filter.SpecificFieldValueFilter
import org.wumiguo.ser.flow.configuration.DataSetConfiguration
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}

/**
 * @author levinliu
 *         Created on 2020/9/8
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object ERResultRender extends Serializable {
  val log = LoggerFactory.getLogger(this.getClass.getName)
  val debug = true

  /**
   * load data with post filter option + pre-filter option, which suite for the case that both filter fields and join fields are indexed
   *
   * @param dataSet1
   * @param dataSet2
   * @param secondEPStartID
   * @param matchDetails
   * @param profiles
   * @param matchedPairs
   * @param showSimilarity
   * @return
   */
  def renderResultV2(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration, secondEPStartID: Int,
                     matchDetails: RDD[(Int, Int, Double)], profiles: RDD[Profile], matchedPairs: RDD[(Int, Int, Double)],
                     showSimilarity: Boolean): (Seq[String], RDD[Row]) = {
    log.info("showSimilarity=" + showSimilarity)
    if (showSimilarity) {
      log.info("matchedPairsWithSimilarityCount=" + matchedPairs.count())
      val profileMatches2 = mapMatchesWithProfilesAndSimilarity(matchedPairs, profiles, secondEPStartID)
      log.info("profileMatchesCount=" + profileMatches2.count())
      val matchesInDiffDataSet = profileMatches2.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
      log.info("matchesInDiffDataSet size =" + matchesInDiffDataSet.count())
      val profilePairMap = matchesInDiffDataSet.map(x => (x._1._1.originalID, x._1._2.originalID, x._1._3))
      log.info("profilePairMap size =" + profilePairMap.count())
      val finalProfiles1: RDD[Profile] = loadAndFilterProfilesByIDPairWithSimilarity(0, dataSet1, profilePairMap)
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val finalProfiles2: RDD[Profile] = loadAndFilterProfilesByIDPairWithSimilarity(1, dataSet2, profilePairMap)
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
      val rows: RDD[Row] = resolveRowsByOriginalIdWithSimilarity(profilePairMap, dataSet1, dataSet2, p1B, p2B)
      (columnNames, rows)
    } else {
      val profileMatches = mapMatchesWithProfilesAndSimilarity(matchedPairs, profiles, secondEPStartID)
      if (!profileMatches.isEmpty()) {
        profileMatches.take(3).foreach(x => log.info("profileMatches=" + x))
      }
      log.info("profileMatchesCount=" + profileMatches.count())
      val matchesInDiffDataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
      log.info("[SSJoin] Get matched pairs " + matchesInDiffDataSet.count())
      if (!matchesInDiffDataSet.isEmpty()) {
        matchesInDiffDataSet.take(3).foreach(t => {
          log.info("matches-pair=" +
            (t._2, (t._1._1.originalID, t._1._1.sourceId), (t._1._2.originalID, t._1._2.sourceId)))
        })
      }
      log.info("matchesInDiffDataSet size =" + matchesInDiffDataSet.count())
      val profilePairMap = matchesInDiffDataSet.map(x => (x._1._1.originalID, x._1._2.originalID))
      log.info("profilePairMap size =" + profilePairMap.count())
      val finalProfiles1: RDD[Profile] = loadAndFilterProfilesByIDPair(0, dataSet1, profilePairMap)
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val finalProfiles2: RDD[Profile] = loadAndFilterProfilesByIDPair(1, dataSet2, profilePairMap)
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      printSomeProfiles(finalProfiles1, finalProfiles2)
      val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
      val rows: RDD[Row] = resolveRowsWithoutSimilarity(profilePairMap, dataSet1, dataSet2, p1B, p2B)
      (columnNames, rows)
    }
  }


  private def resolveRowsWithoutSimilarity(finalMap: RDD[(String, String)],
                                           dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration,
                                           p1B: Broadcast[Array[Profile]], p2B: Broadcast[Array[Profile]]) = {
    val rows = finalMap.map(x => {
      var entry = Seq[String]()
      entry :+= x._1
      val attr1 = p1B.value.filter(p => p.originalID == x._1).flatMap(p => p.attributes)
      entry ++= dataSet1.additionalAttrs.map(ma => attr1.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
      entry :+= x._2
      val attr2 = p2B.value.filter(p => p.originalID == x._2).flatMap(p => p.attributes)
      entry ++= dataSet2.additionalAttrs.map(ma => attr2.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
      Row.fromSeq(entry)
    })
    unpersist(p1B, p2B)
    rows
  }

  /**
   * When the joint fields dont have index, suggest to use this render method to continue with profiles in memory
   *
   * @param dataSet1
   * @param dataSet2
   * @param secondEPStartID
   * @param matchDetails
   * @param profiles
   * @param matchedPairs
   * @param showSimilarity
   * @param profiles1
   * @param profiles2
   * @return
   */
  def renderResultWithPreloadProfiles(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration, secondEPStartID: Int,
                                      matchDetails: RDD[(Int, Int, Double)], profiles: RDD[Profile], matchedPairs: RDD[(Int, Int)],
                                      showSimilarity: Boolean, profiles1: RDD[Profile], profiles2: RDD[Profile]): (Seq[String], RDD[Row]) = {
    if (debug) {
      log.info("renderResultWithPreloadProfiles - dataSet1=" + dataSet1 +
        ",dataSet2=" + dataSet2 +
        ",secondEPStartID=" + secondEPStartID +
        ",matchDetails=" + matchDetails.collect().toList +
        ",profiles=" + profiles.collect().toList +
        ",matchedPairs=" + matchedPairs.collect().toList +
        ",showSimilarity=" + showSimilarity +
        ",profiles1=" + profiles1.collect().toList +
        ",profiles2=" + profiles2.collect().toList)
    }
    val matchedPairsWithSimilarity = enrichPairs(matchedPairs)
    renderResultWithPreloadProfilesAndSimilarityPairs(
      dataSet1, dataSet2, secondEPStartID,
      matchDetails, profiles, matchedPairsWithSimilarity,
      showSimilarity, profiles1, profiles2
    )
  }

  def checkBothIdFieldsProvided(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration): Boolean = {
    dataSet1.idField != null && dataSet2.idField != null && dataSet1.idField != "" && dataSet2.idField != ""
  }

  def resolveIdMaps(profilesMap: RDD[((Profile, Profile), Long)], idFieldsProvided: Boolean): RDD[(String, String)] = {
    val profilePairMap = if (idFieldsProvided) {
      profilesMap.map(x => (x._1._1.originalID, x._1._2.originalID))
    } else {
      profilesMap.map(x => (x._1._1.id.toString, x._1._2.id.toString))
    }
    profilePairMap
  }

  def resolveIdMapsWithSimilarity(profilesMap: RDD[((Profile, Profile, Double), Long)], idFieldsProvided: Boolean): RDD[(String, String, Double)] = {
    val profilePairMap = if (idFieldsProvided) {
      profilesMap.map(x => (x._1._1.originalID, x._1._2.originalID, x._1._3))
    } else {
      profilesMap.map(x => (x._1._1.id.toString, x._1._2.id.toString, x._1._3))
    }
    profilePairMap
  }

  def renderResultWithPreloadProfilesAndSimilarityPairs(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration, secondEPStartID: Int,
                                                        matchDetails: RDD[(Int, Int, Double)], profiles: RDD[Profile], matchedPairsWithSimilarity: RDD[(Int, Int, Double)],
                                                        showSimilarity: Boolean, profiles1: RDD[Profile], profiles2: RDD[Profile]): (Seq[String], RDD[Row]) = {
    log.info("showSimilarity=" + showSimilarity)
    log.info("first profile=" + profiles1.first() + "," + profiles2.first())
    val idFieldsProvided = checkBothIdFieldsProvided(dataSet1, dataSet2)
    log.info("idFieldsProvided=" + idFieldsProvided)
    val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
    if (showSimilarity) {
      log.info("matchedPairsWithSimilarityCount=" + matchedPairsWithSimilarity.count())
      val profileMatches2 = mapMatchesWithProfilesAndSimilarity(matchedPairsWithSimilarity, profiles, secondEPStartID)
      log.info("profileMatchesCount=" + profileMatches2.count())
      val matchesInDiffDataSet = profileMatches2.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
      if (debug) {
        matchesInDiffDataSet.foreach(x => log.info("matchesInDiffDataSetX=" + x))
      }
      log.info("matchesInDiffDataSet size=" + matchesInDiffDataSet.count())
      val profilePairMap = resolveIdMapsWithSimilarity(matchesInDiffDataSet, idFieldsProvided)
      if (debug) {
        profilePairMap.foreach(x => log.info("profilePairx=" + x))
      }
      log.info("profilePairMap size=" + profilePairMap.count())

      val (trimDownProfiles1, trimDownProfiles2) = if (idFieldsProvided) {
        (
          trimDownByOringinalIdWithSimilarity(0, dataSet1, profiles1, profilePairMap),
          trimDownByOringinalIdWithSimilarity(1, dataSet2, profiles2, profilePairMap)
        )
      } else {
        (
          trimDownByProfileIdWithSimilarity(0, dataSet1, profiles1, profilePairMap),
          trimDownByProfileIdWithSimilarity(1, dataSet2, profiles2, profilePairMap)
        )
      }
      if (debug) {
        trimDownProfiles1.foreach(x => log.info("trimDownProfile1x=" + x))
        trimDownProfiles2.foreach(x => log.info("trimDownProfile2x=" + x))
      }
      val p1B = matchDetails.sparkContext.broadcast(trimDownProfiles1.collect())
      val p2B = matchDetails.sparkContext.broadcast(trimDownProfiles2.collect())
      val rows: RDD[Row] = if (idFieldsProvided) {
        resolveRowsByOriginalIdWithSimilarity(profilePairMap, dataSet1, dataSet2, p1B, p2B)
      } else {
        resolveRowsByProfileIdWithSimilarity(profilePairMap, dataSet1, dataSet2, p1B, p2B)
      }
      (columnNames, rows)
    } else {
      val profileMatches = mapMatchesWithProfiles(matchedPairsWithSimilarity.map(x => (x._1, x._2)), profiles, secondEPStartID)
      if (!profileMatches.isEmpty()) {
        profileMatches.take(3).foreach(x => log.info("profileMatches=" + x))
      }
      log.info("profileMatchesCount=" + profileMatches.count())
      val matchesInDiffDataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
      log.info("[SSJoin] Get matched pairs " + matchesInDiffDataSet.count())
      if (!matchesInDiffDataSet.isEmpty()) {
        matchesInDiffDataSet.take(3).foreach(t => {
          log.info("matches-pair=" +
            (t._2, (t._1._1.originalID, t._1._1.sourceId), (t._1._2.originalID, t._1._2.sourceId)))
        })
      }
      log.info("matchesInDiffDataSet size =" + matchesInDiffDataSet.count())
      val profilePairMap = resolveIdMaps(matchesInDiffDataSet, idFieldsProvided)
      if (debug) {
        profilePairMap.foreach(x => log.info("profilePairx=" + x))
      }
      log.info("profilePairMap size =" + profilePairMap.count())
      val trimDownProfiles1: RDD[Profile] = trimDownByOriginalId(0, dataSet1, profiles1, profilePairMap)
      val trimDownProfiles2: RDD[Profile] = trimDownByOriginalId(1, dataSet2, profiles2, profilePairMap)
      val p1B = matchDetails.sparkContext.broadcast(trimDownProfiles1.collect())
      val p2B = matchDetails.sparkContext.broadcast(trimDownProfiles2.collect())
      val rows: RDD[Row] = resolveRowsWithoutSimilarity(profilePairMap, dataSet1, dataSet2, p1B, p2B)
      (columnNames, rows)
    }
  }

  private def trimDownByOringinalIdWithSimilarity(sourceId: Int, dataSet: DataSetConfiguration, profiles: RDD[Profile], profilePairMap: RDD[(String, String, Double)]) = {
    trimDownByOriginalId(sourceId, dataSet, profiles, profilePairMap.map(x => (x._1, x._2)))
  }

  private def trimDownByProfileIdWithSimilarity(sourceId: Int, dataSet: DataSetConfiguration, profiles: RDD[Profile], profilePairMap: RDD[(String, String, Double)]) = {
    trimDownByProfileId(sourceId, dataSet, profiles, profilePairMap.map(x => (x._1, x._2)))
  }

  private def trimDownByProfileId(sourceId: Int, dataSet: DataSetConfiguration, profiles: RDD[Profile], profilePairMap: RDD[(String, String)]) = {
    val idSet = profilePairMap.map(x => if (sourceId == 0) {
      x._1
    } else {
      x._2
    }).toLocalIterator.toSet
    if (debug) {
      log.info("trimDownByProfileId - sourceId=" + sourceId + ",idSet=" + idSet)
    }
    val trimDownProfiles = profiles.filter(x => idSet.contains(x.id.toString)).map(x =>
      Profile(x.id, x.attributes.filter(y => dataSet.additionalAttrs.contains(y.key)), x.originalID, x.sourceId))
    trimDownProfiles
  }


  private def trimDownByOriginalId(sourceId: Int, dataSet: DataSetConfiguration, profiles: RDD[Profile], profilePairMap: RDD[(String, String)]) = {
    val idSet = profilePairMap.map(x => if (sourceId == 0) {
      x._1
    } else {
      x._2
    }).toLocalIterator.toSet
    if (debug) {
      log.info("trimDownByOriginalId - sourceId=" + sourceId + ",idSet=" + idSet)
    }
    val trimDownProfiles = profiles.filter(x => idSet.contains(x.originalID)).map(x =>
      Profile(x.id, x.attributes.filter(y => y.key == dataSet.idField || dataSet.additionalAttrs.contains(y.key)), x.originalID, x.sourceId))
    trimDownProfiles
  }

  def renderResult(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration, secondEPStartID: Int,
                   matchDetails: RDD[(Int, Int, Double)], profiles: RDD[Profile], matchedPairs: RDD[(Int, Int)],
                   showSimilarity: Boolean): (Seq[String], RDD[Row]) = {
    log.info("showSimilarity=" + showSimilarity)
    if (showSimilarity) {
      val matchedPairsWithSimilarity = enrichPairs(matchedPairs)
      log.info("matchedPairsWithSimilarityCount=" + matchedPairsWithSimilarity.count())
      val profileMatches2 = mapMatchesWithProfilesAndSimilarity(matchedPairsWithSimilarity, profiles, secondEPStartID)
      log.info("profileMatchesCount=" + profileMatches2.count())
      val matchesInDiffDataSet = profileMatches2.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
      log.info("matchesInDiffDataSet size =" + matchesInDiffDataSet.count())
      val profilePairMap = matchesInDiffDataSet.map(x => (x._1._1.originalID, x._1._2.originalID, x._1._3))
      log.info("profilePairMap size =" + profilePairMap.count())
      val finalProfiles1: RDD[Profile] = loadAndFilterProfilesByIDPairWithSimilarity(0, dataSet1, profilePairMap)
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val finalProfiles2: RDD[Profile] = loadAndFilterProfilesByIDPairWithSimilarity(1, dataSet2, profilePairMap)
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      printSomeProfiles(finalProfiles1, finalProfiles2)
      val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
      val rows: RDD[Row] = resolveRowsByOriginalIdWithSimilarity(profilePairMap, dataSet1, dataSet2, p1B, p2B)
      (columnNames, rows)
    } else {
      val profileMatches = mapMatchesWithProfiles(matchedPairs, profiles, secondEPStartID)
      if (!profileMatches.isEmpty()) {
        profileMatches.take(3).foreach(x => log.info("profileMatches=" + x))
      }
      log.info("profileMatchesCount=" + profileMatches.count())
      val matchesInDiffDataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
      log.info("[SSJoin] Get matched pairs " + matchesInDiffDataSet.count())
      if (!matchesInDiffDataSet.isEmpty()) {
        matchesInDiffDataSet.take(3).foreach(t => {
          log.info("matches-pair=" +
            (t._2, (t._1._1.originalID, t._1._1.sourceId), (t._1._2.originalID, t._1._2.sourceId)))
        })
      }
      log.info("matchesInDiffDataSet size =" + matchesInDiffDataSet.count())
      val profilePairMap = matchesInDiffDataSet.map(x => (x._1._1.originalID, x._1._2.originalID))
      log.info("profilePairMap size =" + profilePairMap.count())
      val finalProfiles1: RDD[Profile] = loadAndFilterProfilesByIDPair(0, dataSet1, profilePairMap)
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val finalProfiles2: RDD[Profile] = loadAndFilterProfilesByIDPair(1, dataSet2, profilePairMap)
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      printSomeProfiles(finalProfiles1, finalProfiles2)
      val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
      val rows: RDD[Row] = resolveRowsWithoutSimilarity(profilePairMap, dataSet1, dataSet2, p1B, p2B)
      (columnNames, rows)
    }
  }

  private def unpersist(p1B: Broadcast[Array[Profile]], p2B: Broadcast[Array[Profile]]): Unit = {
    if (p1B != null) {
      p1B.unpersist()
    }
    if (p2B != null) {
      p2B.unpersist()
    }
  }

  private def loadAndFilterProfilesByIDPairWithSimilarity(sourceId: Int, dataSetConf: DataSetConfiguration, profilePairMap: RDD[(String, String, Double)]) = {
    loadAndFilterProfilesByIDPair(sourceId, dataSetConf, profilePairMap.map(x => (x._1, x._2)))
  }

  private def loadAndFilterProfilesByIDPair(sourceId: Int, dataSetConf: DataSetConfiguration, profilePairMap: RDD[(String, String)]) = {
    val idFilterOption = profilePairMap.map(x => KeyValue(dataSetConf.idField, if (sourceId == 0) {
      x._1
    } else {
      x._2
    })).toLocalIterator.toList
    val finalFilterOption = (dataSetConf.filterOptions.filter(_.key != dataSetConf.idField) ++ idFilterOption).toList
    val finalProfiles = getProfileLoader(dataSetConf.path).load(dataSetConf.path, realIDField = dataSetConf.idField,
      startIDFrom = 0, sourceId = sourceId, keepRealID = dataSetConf.includeRealID, fieldsToKeep = dataSetConf.additionalAttrs.toList,
      fieldValuesScope = finalFilterOption,
      filter = SpecificFieldValueFilter
    )
    finalProfiles
  }


  private def resolveRowsByProfileIdWithSimilarity(finalMap2: RDD[(String, String, Double)],
                                                   dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration,
                                                   p1B: Broadcast[Array[Profile]], p2B: Broadcast[Array[Profile]]) = {
    val rows = finalMap2.map(x => {
      var entry = Seq[String]()
      //similarity : _3
      entry :+= x._3.toString
      entry :+= x._1
      val attr1 = p1B.value.filter(p => p.id.toString == x._1).flatMap(p => p.attributes)
      entry ++= dataSet1.additionalAttrs.map(ma => attr1.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value)
      entry :+= x._2
      val attr2 = p2B.value.filter(p => p.id.toString == x._2).flatMap(p => p.attributes)
      log.info("attr1=" + attr1.toSeq)
      log.info("attr2=" + attr2.toSeq)
      entry ++= dataSet2.additionalAttrs.map(ma => attr2.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value)
      Row.fromSeq(entry)
    })
    unpersist(p1B, p2B)
    rows
  }

  private def resolveRowsByOriginalIdWithSimilarity(finalMap2: RDD[(String, String, Double)],
                                                    dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration,
                                                    p1B: Broadcast[Array[Profile]], p2B: Broadcast[Array[Profile]]) = {
    val rows = finalMap2.map(x => {
      var entry = Seq[String]()
      //similarity : _3
      entry :+= x._3.toString
      entry :+= x._1
      val attr1 = p1B.value.filter(p => p.originalID == x._1).flatMap(p => p.attributes)
      entry ++= dataSet1.additionalAttrs.map(ma => attr1.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
      entry :+= x._2
      val attr2 = p2B.value.filter(p => p.originalID == x._2).flatMap(p => p.attributes)
      entry ++= dataSet2.additionalAttrs.map(ma => attr2.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
      Row.fromSeq(entry)
    })
    unpersist(p1B, p2B)
    rows
  }

  private def printSomeProfiles(finalProfiles1: RDD[Profile], finalProfiles2: RDD[Profile]) = {
    if (!finalProfiles1.isEmpty()) {
      finalProfiles1.take(3).foreach(x => log.info("fp1=" + x))
    }
    if (!finalProfiles2.isEmpty()) {
      finalProfiles2.take(3).foreach(x => log.info("fp2=" + x))
    }
    log.info("fp1count=" + finalProfiles1.count())
    log.info("fp2count=" + finalProfiles2.count())
  }

  def mapMatchesWithProfilesAndSimilarity(matchedPairs: RDD[(Int, Int, Double)], profiles: RDD[Profile], secondEPStartID: Int): RDD[(Profile, Profile, Double)] = {
    val profilesById = profiles.keyBy(_.id)
    val matchedPairsById = matchedPairs.keyBy(_._1)
    val joinResult = matchedPairsById.join(profilesById)
    joinResult.
      map(t => (t._2._1._2, t._2._2, t._2._1._3)).keyBy(_._1).
      join(profilesById).map(t => {
      val id1 = t._2._1._2
      val id2 = t._2._2
      val similarity = t._2._1._3
      if (id1.id < secondEPStartID) {
        (id1, id2, similarity)
      } else {
        (id2, id1, similarity)
      }
    })
  }

  def mapMatchesWithProfiles(matchedPairs: RDD[(Int, Int)], profiles: RDD[Profile], secondEPStartID: Int): RDD[(Profile, Profile)] = {
    val profilesById = profiles.keyBy(_.id)
    val matchedPairsById = matchedPairs.keyBy(_._1)
    val joinResult = matchedPairsById.join(profilesById)
    joinResult.
      map(t => (t._2._1._2, t._2._2)).keyBy(_._1).
      join(profilesById).map(t => {
      val id1 = t._2._1._2
      val id2 = t._2._2
      if (id1.id < secondEPStartID) {
        (id1, id2)
      } else {
        (id2, id1)
      }
    })
  }


  private def resolveColumns(moreAttr1s: Seq[String], moreAttr2s: Seq[String], showSimilarity: Boolean = false) = {
    var columnNames = Seq[String]()
    if (showSimilarity) {
      columnNames :+= "Similarity"
    }
    val profile1Prefix = "P1-"
    val profile2Prefix = "P2-"
    columnNames :+= profile1Prefix + "ID"
    columnNames ++= moreAttr1s.map(x => profile1Prefix + x)
    columnNames :+= profile2Prefix + "ID"
    columnNames ++= moreAttr2s.map(x => profile2Prefix + x)
    columnNames
  }


  private def enrichPairs(matchPairs: RDD[(Int, Int)]): RDD[(Int, Int, Double)] = {
    matchPairs.map(x => (x._1, x._2, 1.0))
  }

  private def enrichWithSimilarity(matchPairs: RDD[(Int, Int)], matchDetails: RDD[(Int, Int, Double)], secondEPStartID: Int): RDD[(Int, Int, Double)] = {
    val mp = matchPairs.keyBy(x => (x._1, x._2))
    val detail = matchDetails.keyBy(x => (x._1, x._2))
    val data = mp.join(detail)
    data.map(x => x._2._2)
  }

  private def getProfileLoader(dataFile: String): ProfileLoaderTrait = {
    ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(dataFile))
  }

}
