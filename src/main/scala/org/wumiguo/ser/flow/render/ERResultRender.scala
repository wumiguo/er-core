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
  def posLoadThenRenderResult(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration, secondEPStartID: Int,
                              matchDetails: RDD[(Int, Int, Double)], profiles: RDD[Profile], matchedPairs: RDD[(Int, Int, Double)],
                              showSimilarity: Boolean): (Seq[String], RDD[Row]) = {
    log.info("showSimilarity=" + showSimilarity)
    val idFieldsProvided = checkBothIdFieldsProvided(dataSet1, dataSet2)
    log.info("idFieldsProvided=" + idFieldsProvided)
    val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
    if (showSimilarity) {
      log.info("matchedPairsWithSimilarityCount=" + matchedPairs.count())
      val profileMatches = mapMatchesWithProfilesAndSimilarity(matchedPairs, profiles, secondEPStartID)
      log.info("profileMatchesCount=" + profileMatches.count())
      val profilePairMap = profileMapAsIdMapWithSimilarity(profileMatches, idFieldsProvided)
      log.info("profilePairMap size =" + profilePairMap.count())
      val finalProfiles1: RDD[Profile] = loadAndFilterProfilesByIDPairWithSimilarity(0, dataSet1, profilePairMap)
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val finalProfiles2: RDD[Profile] = loadAndFilterProfilesByIDPairWithSimilarity(1, dataSet2, profilePairMap)
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      val rows: RDD[Row] = resolveRowsWithSimilarity(profilePairMap, idFieldsProvided, dataSet1, dataSet2, p1B, p2B, secondEPStartID)
      (columnNames, rows)
    } else {
      val profileMatches = mapMatchesWithProfiles(matchedPairs.map(x => (x._1, x._2)), profiles, secondEPStartID)
      log.info("profileMatchesCount=" + profileMatches.count())
      val profilePairMap = profileMapAsIdMap(profileMatches, idFieldsProvided)
      log.info("profilePairMap size =" + profilePairMap.count())
      val finalProfiles1: RDD[Profile] = loadAndFilterProfilesByIDPair(0, dataSet1, profilePairMap)
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val finalProfiles2: RDD[Profile] = loadAndFilterProfilesByIDPair(1, dataSet2, profilePairMap)
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      printSomeProfiles(finalProfiles1, finalProfiles2)
      val rows: RDD[Row] = resolveRows(profilePairMap, idFieldsProvided, dataSet1, dataSet2, p1B, p2B, secondEPStartID)
      (columnNames, rows)
    }
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

  private def checkBothIdFieldsProvided(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration): Boolean = {
    dataSet1.idField != null && dataSet2.idField != null && dataSet1.idField != "" && dataSet2.idField != ""
  }

  def profileMapAsIdMap(profileMatches: RDD[(Profile, Profile)], idFieldsProvided: Boolean): RDD[(String, String)] = {
    if (debug && !profileMatches.isEmpty()) {
      profileMatches.take(3).foreach(x => log.info("profileMatches=" + x))
    }
    val matchesInDiffDataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
    if (debug) {
      matchesInDiffDataSet.foreach(x => log.info("matchesInDiffDataSetX=" + x))
    }
    log.info("[SSJoin] Get matched pairs " + matchesInDiffDataSet.count())
    resolveIdMaps(matchesInDiffDataSet, idFieldsProvided)
  }

  /**
   * the profilePairMap can be:
   * (originalProfile1Id, originalProfile2Id) when id fields both provided,
   * Or (profile1Id, profile2Id)
   *
   * @param profilesMap
   * @param idFieldsProvided
   * @return
   */
  def resolveIdMaps(profilesMap: RDD[((Profile, Profile), Long)], idFieldsProvided: Boolean): RDD[(String, String)] = {
    val profilePairMap = if (idFieldsProvided) {
      profilesMap.map(x => (x._1._1.originalID, x._1._2.originalID))
    } else {
      profilesMap.map(x => (x._1._1.id.toString, x._1._2.id.toString))
    }
    profilePairMap
  }


  def profileMapAsIdMapWithSimilarity(profileMatches: RDD[(Profile, Profile, Double)], idFieldsProvided: Boolean): RDD[(String, String, Double)] = {
    val matchesInDiffDataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
    log.info("[SSJoin] Get matched pairs " + matchesInDiffDataSet.count())
    resolveIdMapsWithSimilarity(matchesInDiffDataSet, idFieldsProvided)
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
    if (debug) {
      log.info("first profile=" + profiles1.first() + "," + profiles2.first())
    }
    val idFieldsProvided = checkBothIdFieldsProvided(dataSet1, dataSet2)
    log.info("idFieldsProvided=" + idFieldsProvided)
    val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
    if (showSimilarity) {
      log.info("matchedPairsWithSimilarityCount=" + matchedPairsWithSimilarity.count())
      val profileMatches = mapMatchesWithProfilesAndSimilarity(matchedPairsWithSimilarity, profiles, secondEPStartID)
      log.info("profileMatchesCount=" + profileMatches.count())
      val profilePairMap = profileMapAsIdMapWithSimilarity(profileMatches, idFieldsProvided)
      if (debug) {
        profilePairMap.foreach(x => log.info("profilePairx=" + x))
      }
      log.info("profilePairMap size=" + profilePairMap.count())
      val (trimDownProfiles1, trimDownProfiles2) = (
        trimDownByIdWithSimilarity(0, dataSet1, profiles1, profilePairMap, idFieldsProvided),
        trimDownByIdWithSimilarity(1, dataSet2, profiles2, profilePairMap, idFieldsProvided)
      )
      if (debug) {
        trimDownProfiles1.foreach(x => log.info("trimDownProfile1x=" + x))
        trimDownProfiles2.foreach(x => log.info("trimDownProfile2x=" + x))
      }
      val p1B = matchDetails.sparkContext.broadcast(trimDownProfiles1.collect())
      val p2B = matchDetails.sparkContext.broadcast(trimDownProfiles2.collect())
      val rows: RDD[Row] = resolveRowsWithSimilarity(profilePairMap, idFieldsProvided, dataSet1, dataSet2, p1B, p2B, secondEPStartID)
      (columnNames, rows)
    } else {
      val profileMatches = mapMatchesWithProfiles(matchedPairsWithSimilarity.map(x => (x._1, x._2)), profiles, secondEPStartID)
      log.info("profileMatchesCount=" + profileMatches.count())
      val profilePairMap = profileMapAsIdMap(profileMatches, idFieldsProvided)
      if (debug) {
        profilePairMap.foreach(x => log.info("profilePairx=" + x))
      }
      log.info("profilePairMap size =" + profilePairMap.count())
      val trimDownProfiles1: RDD[Profile] = trimDownById(0, dataSet1, profiles1, profilePairMap, idFieldsProvided)
      val trimDownProfiles2: RDD[Profile] = trimDownById(1, dataSet2, profiles2, profilePairMap, idFieldsProvided)
      val p1B = matchDetails.sparkContext.broadcast(trimDownProfiles1.collect())
      val p2B = matchDetails.sparkContext.broadcast(trimDownProfiles2.collect())
      val rows: RDD[Row] = resolveRows(profilePairMap, idFieldsProvided, dataSet1, dataSet2, p1B, p2B, secondEPStartID)
      (columnNames, rows)
    }
  }

  private def trimDownByIdWithSimilarity(sourceId: Int, dataSet: DataSetConfiguration, profiles: RDD[Profile], profilePairMap: RDD[(String, String, Double)], idFieldsProvided: Boolean) = {
    trimDownById(sourceId, dataSet, profiles, profilePairMap.map(x => (x._1, x._2)), idFieldsProvided)
  }

  private def trimDownById(sourceId: Int, dataSet: DataSetConfiguration, profiles: RDD[Profile], profilePairMap: RDD[(String, String)], idFieldsProvided: Boolean) = {
    val idSet = profilePairMap.map(x => if (sourceId == 0) {
      x._1
    } else {
      x._2
    }).distinct()
    val idSetB = profiles.sparkContext.broadcast(idSet.collect())
    if (debug) {
      log.info("trimDownByProfileId - sourceId=" + sourceId + ",idSet=" + idSet)
    }
    val trimDownProfiles =
      if (idFieldsProvided) {
        profiles.filter(x => idSetB.value.contains(x.originalID)).map(x =>
          Profile(x.id, x.attributes.filter(y => y.key == dataSet.idField || dataSet.additionalAttrs.contains(y.key)), x.originalID, x.sourceId))
      } else {
        profiles.filter(x => idSetB.value.contains(x.id.toString)).map(x =>
          Profile(x.id, x.attributes.filter(y => dataSet.additionalAttrs.contains(y.key)), x.originalID, x.sourceId))
      }
    idSetB.unpersist()
    trimDownProfiles
  }

  def renderResult(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration, secondEPStartID: Int,
                   matchDetails: RDD[(Int, Int, Double)], profiles: RDD[Profile], matchedPairs: RDD[(Int, Int)],
                   showSimilarity: Boolean): (Seq[String], RDD[Row]) = {
    log.info("showSimilarity=" + showSimilarity)
    val idFieldsProvided = checkBothIdFieldsProvided(dataSet1, dataSet2)
    log.info("idFieldsProvided=" + idFieldsProvided)
    val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
    if (showSimilarity) {
      val matchedPairsWithSimilarity = enrichPairs(matchedPairs)
      log.info("matchedPairsWithSimilarityCount=" + matchedPairsWithSimilarity.count())
      val profileMatches = mapMatchesWithProfilesAndSimilarity(matchedPairsWithSimilarity, profiles, secondEPStartID)
      log.info("profileMatchesCount=" + profileMatches.count())
      val profilePairMap = profileMapAsIdMapWithSimilarity(profileMatches, idFieldsProvided)
      log.info("profilePairMap size =" + profilePairMap.count())
      val finalProfiles1: RDD[Profile] = loadAndFilterProfilesByIDPairWithSimilarity(0, dataSet1, profilePairMap)
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val finalProfiles2: RDD[Profile] = loadAndFilterProfilesByIDPairWithSimilarity(1, dataSet2, profilePairMap)
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      printSomeProfiles(finalProfiles1, finalProfiles2)
      val rows: RDD[Row] = resolveRowsWithSimilarity(profilePairMap, idFieldsProvided, dataSet1, dataSet2, p1B, p2B, secondEPStartID)
      (columnNames, rows)
    } else {
      val profileMatches = mapMatchesWithProfiles(matchedPairs, profiles, secondEPStartID)
      if (debug && !profileMatches.isEmpty()) {
        profileMatches.take(3).foreach(x => log.info("profileMatches=" + x))
      }
      log.info("profileMatchesCount=" + profileMatches.count())
      val profilePairMap = profileMapAsIdMap(profileMatches, idFieldsProvided)
      log.info("profilePairMap size =" + profilePairMap.count())
      val finalProfiles1: RDD[Profile] = loadAndFilterProfilesByIDPair(0, dataSet1, profilePairMap)
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val finalProfiles2: RDD[Profile] = loadAndFilterProfilesByIDPair(1, dataSet2, profilePairMap)
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      printSomeProfiles(finalProfiles1, finalProfiles2)
      val rows: RDD[Row] = resolveRows(profilePairMap, idFieldsProvided, dataSet1, dataSet2, p1B, p2B, secondEPStartID)
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
    }))
    val idFilterOptionB = profilePairMap.sparkContext.broadcast(idFilterOption.collect())
    val finalFilterOption = (dataSetConf.filterOptions.filter(_.key != dataSetConf.idField) ++ idFilterOptionB.value).toList
    val finalProfiles = getProfileLoader(dataSetConf.path).load(dataSetConf.path, realIDField = dataSetConf.idField,
      startIDFrom = 0, sourceId = sourceId, keepRealID = dataSetConf.includeRealID, fieldsToKeep = dataSetConf.additionalAttrs.toList,
      fieldValuesScope = finalFilterOption,
      filter = SpecificFieldValueFilter
    )
    idFilterOptionB.unpersist()
    finalProfiles
  }


  private def resolveRows(finalMap: RDD[(String, String)], idFieldsProvided: Boolean,
                          dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration,
                          p1B: Broadcast[Array[Profile]], p2B: Broadcast[Array[Profile]],
                          secondEPStartID: Int) = {
    val rows = finalMap.map(x => {
      var entry = Seq[String]()
      entry :+= x._1
      entry ++= retrieveAdditionalAttr(x._1, p1B, idFieldsProvided, dataSet1)
      entry :+= adjustProfile2RelativeId(x._2, secondEPStartID, idFieldsProvided)
      entry ++= retrieveAdditionalAttr(x._2, p2B, idFieldsProvided, dataSet2)
      Row.fromSeq(entry)
    })
    unpersist(p1B, p2B)
    rows
  }


  private def resolveRowsWithSimilarity(finalMap: RDD[(String, String, Double)],
                                        idFieldsProvided: Boolean,
                                        dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration,
                                        p1B: Broadcast[Array[Profile]], p2B: Broadcast[Array[Profile]],
                                        secondEPStartID: Int) = {
    val rows = finalMap.map(x => {
      var entry = Seq[String]()
      //similarity : _3
      entry :+= x._3.toString
      //profile1 ID field
      entry :+= x._1
      entry ++= retrieveAdditionalAttr(x._1, p1B, idFieldsProvided, dataSet1)
      //profile2 ID field
      entry :+= adjustProfile2RelativeId(x._2, secondEPStartID, idFieldsProvided)
      entry ++= retrieveAdditionalAttr(x._2, p2B, idFieldsProvided, dataSet2)
      Row.fromSeq(entry)
    })
    unpersist(p1B, p2B)
    rows
  }

  private def adjustProfile2RelativeId(profile2Id: String, secondEPStartID: Int, idFieldsProvided: Boolean): String = {
    if (idFieldsProvided) {
      profile2Id
    } else {
      (profile2Id.toInt - secondEPStartID).toString
    }
  }

  private def retrieveAdditionalAttr(id: String, profiles: Broadcast[Array[Profile]], idFieldsProvided: Boolean, dataSetConf: DataSetConfiguration) = {
    val attrs = if (idFieldsProvided) {
      //using given ID field
      profiles.value.filter(p => p.originalID == id).flatMap(p => p.attributes)
    } else {
      //use profile id//this may be changing all the time
      profiles.value.filter(p => p.id.toString == id).flatMap(p => p.attributes)
    }
    //append additional attrs
    dataSetConf.additionalAttrs.map(ma => attrs.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value)
  }

  private def printSomeProfiles(finalProfiles1: RDD[Profile], finalProfiles2: RDD[Profile]) = {
    if (debug && !finalProfiles1.isEmpty()) {
      finalProfiles1.take(3).foreach(x => log.info("fp1=" + x))
    }
    if (debug && !finalProfiles2.isEmpty()) {
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

  private def getProfileLoader(dataFile: String): ProfileLoaderTrait = {
    ProfileLoaderFactory.getDataLoader(DataTypeResolver.getDataType(dataFile))
  }

}
