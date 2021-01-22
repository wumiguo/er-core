package org.wumiguo.ser.flow.render

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
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
      val profileMatches = mapMatchesWithProfilesAndSimilarity(matchedPairs, profiles, secondEPStartID)
      val profilePairMap = profileMapAsIdMapWithSimilarity(profileMatches, idFieldsProvided)
      val finalProfiles1: RDD[Profile] = loadAndFilterProfilesByIDPairWithSimilarity(0, dataSet1, profilePairMap)
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val finalProfiles2: RDD[Profile] = loadAndFilterProfilesByIDPairWithSimilarity(1, dataSet2, profilePairMap)
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      val rows: RDD[Row] = resolveRowsWithSimilarity(profilePairMap, idFieldsProvided, dataSet1, dataSet2, p1B, p2B, secondEPStartID)
      (columnNames, rows)
    } else {
      val profileMatches = mapMatchesWithProfiles(matchedPairs.map(x => (x._1, x._2)), profiles, secondEPStartID)
      val profilePairMap = profileMapAsIdMap(profileMatches, idFieldsProvided)
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
   * @param profiles
   * @param matchedPairs
   * @param showSimilarity
   * @param profiles1
   * @param profiles2
   * @return
   */
  def renderResultWithPreloadProfiles(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration, secondEPStartID: Int,
                                      profiles: RDD[Profile], matchedPairs: RDD[(Int, Int)],
                                      showSimilarity: Boolean, profiles1: RDD[Profile], profiles2: RDD[Profile]): (Seq[String], RDD[Row]) = {
    if (debug) {
      log.info("renderResultWithPreloadProfiles - dataSet1=" + dataSet1 +
        ",dataSet2=" + dataSet2 +
        ",secondEPStartID=" + secondEPStartID +
        ",profiles=" + profiles.collect().toList +
        ",matchedPairs=" + matchedPairs.collect().toList +
        ",showSimilarity=" + showSimilarity +
        ",profiles1=" + profiles1.collect().toList +
        ",profiles2=" + profiles2.collect().toList)
    }
    val matchedPairsWithSimilarity = enrichPairs(matchedPairs)
    renderResultWithPreloadProfilesAndSimilarityPairsV2(
      dataSet1, dataSet2, secondEPStartID,
      profiles, matchedPairsWithSimilarity,
      showSimilarity, profiles1, profiles2
    )
  }

  def checkBothIdFieldsProvided(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration): Boolean = {
    dataSet1.idField != null && dataSet2.idField != null && dataSet1.idField != "" && dataSet2.idField != ""
  }

  def profileMapAsIdMap(profileMatches: RDD[(Profile, Profile)], idFieldsProvided: Boolean): RDD[(String, String)] = {
    if (debug && !profileMatches.isEmpty()) {
      profileMatches.take(3).foreach(x => log.info("profileMatches=" + x))
    }
    val matchesInDiffDataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId)
    if (debug) {
      matchesInDiffDataSet.foreach(x => log.info("matchesInDiffDataSetX=" + x))
    }
    log.info("[SSJoin] Get matched pairs " + matchesInDiffDataSet.count())
    profilePairAsIdPair(matchesInDiffDataSet, idFieldsProvided)
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
  def profilePairAsIdPair(profilesMap: RDD[(Profile, Profile)], idFieldsProvided: Boolean): RDD[(String, String)] = {
    val profilePairMap = if (idFieldsProvided) {
      profilesMap.map(x => (x._1.originalID, x._2.originalID))
    } else {
      profilesMap.map(x => (x._1.id.toString, x._2.id.toString))
    }
    profilePairMap
  }


  def profileMapAsIdMapWithSimilarity(profileMatches: RDD[(Profile, Profile, Double)], idFieldsProvided: Boolean): RDD[(String, String, Double)] = {
    val matchesInDiffDataSet = profileMatches.filter(t => t._1.sourceId != t._2.sourceId)
    //log.info("[SSJoin] Get matched pairs " + matchesInDiffDataSet.count())
    resolveIdMapsWithSimilarity(matchesInDiffDataSet, idFieldsProvided)
  }

  def resolveIdMapsWithSimilarity(profilesMatchPair: RDD[(Profile, Profile, Double)], idFieldsProvided: Boolean): RDD[(String, String, Double)] = {
    val profilePairMap = if (idFieldsProvided) {
      profilesMatchPair.map(x => (x._1.originalID, x._2.originalID, x._3))
    } else {
      profilesMatchPair.map(x => (x._1.id.toString, x._2.id.toString, x._3))
    }
    profilePairMap
  }

  def renderResultWithPreloadProfilesAndSimilarityPairs(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration, secondEPStartID: Int,
                                                        profiles: RDD[Profile], matchedPairsWithSimilarity: RDD[(Int, Int, Double)],
                                                        showSimilarity: Boolean, profiles1: RDD[Profile], profiles2: RDD[Profile]): (Seq[String], RDD[Row]) = {
    log.info("showSimilarity=" + showSimilarity)
    if (debug) {
      log.info("first profile=" + profiles1.first() + "," + profiles2.first())
    }
    val idFieldsProvided = checkBothIdFieldsProvided(dataSet1, dataSet2)
    log.info("idFieldsProvided=" + idFieldsProvided)
    val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
    if (showSimilarity) {
      //log.info("matchedPairsWithSimilarityCount=" + matchedPairsWithSimilarity.count())
      val profileMatches = mapMatchesWithProfilesAndSimilarity(matchedPairsWithSimilarity, profiles, secondEPStartID)
      //log.info("profileMatchesCount=" + profileMatches.count())
      val profileMatchPair = profileMapAsIdMapWithSimilarity(profileMatches, idFieldsProvided)
      if (debug) {
        profileMatchPair.foreach(x => log.info("profilePairx=" + x))
      }
      //log.info("profilePairMap size=" + profileMatchPair.count())
      val (trimDownProfiles1, trimDownProfiles2) = (
        trimDownByIdWithSimilarity(0, dataSet1, profiles1, profileMatchPair, idFieldsProvided),
        trimDownByIdWithSimilarity(1, dataSet2, profiles2, profileMatchPair, idFieldsProvided)
      )
      if (debug) {
        trimDownProfiles1.foreach(x => log.info("trimDownProfile1x=" + x))
        trimDownProfiles2.foreach(x => log.info("trimDownProfile2x=" + x))
      }
      val p1B = profileMatchPair.sparkContext.broadcast(trimDownProfiles1.collect())
      val p2B = profileMatchPair.sparkContext.broadcast(trimDownProfiles2.collect())
      val rows: RDD[Row] = resolveRowsWithSimilarity(profileMatchPair, idFieldsProvided, dataSet1, dataSet2, p1B, p2B, secondEPStartID)
      (columnNames, rows)
    } else {
      val profileMatches = mapMatchesWithProfiles(matchedPairsWithSimilarity.map(x => (x._1, x._2)), profiles, secondEPStartID)
      //log.info("profileMatchesCount=" + profileMatches.count())
      val profilePairMap = profileMapAsIdMap(profileMatches, idFieldsProvided)
      if (debug) {
        profilePairMap.foreach(x => log.info("profilePairx=" + x))
      }
      //log.info("profilePairMap size =" + profilePairMap.count())
      val trimDownProfiles1: RDD[Profile] = trimDownById(0, dataSet1, profiles1, profilePairMap, idFieldsProvided)
      val trimDownProfiles2: RDD[Profile] = trimDownById(1, dataSet2, profiles2, profilePairMap, idFieldsProvided)
      val p1B = profileMatches.sparkContext.broadcast(trimDownProfiles1.collect())
      val p2B = profileMatches.sparkContext.broadcast(trimDownProfiles2.collect())
      val rows: RDD[Row] = resolveRows(profilePairMap, idFieldsProvided, dataSet1, dataSet2, p1B, p2B, secondEPStartID)
      (columnNames, rows)
    }
  }


  def renderResultWithPreloadProfilesAndSimilarityPairsV2(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration, secondEPStartID: Int,
                                                          profiles: RDD[Profile], matchedPairsWithSimilarity: RDD[(Int, Int, Double)],
                                                          showSimilarity: Boolean, profiles1: RDD[Profile], profiles2: RDD[Profile]): (Seq[String], RDD[Row]) = {
    log.info("renderResultWithPreloadProfilesAndSimilarityPairsV2")
    log.info("showSimilarity=" + showSimilarity)
    val idFieldProvided = checkBothIdFieldsProvided(dataSet1, dataSet2)
    log.info("idFieldProvided=" + idFieldProvided)
    val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)

    def trimThenSwap(matchedPairsWithSimilarity: RDD[(Int, Int, Double)], secondEPStartId: Int) = {
      var res = matchedPairsWithSimilarity.map(pair => {
        if (pair._1 < secondEPStartID) {
          pair
        } else {
          (pair._2, pair._1, pair._3)
        }
      }).filter(p => p._1 < secondEPStartID && p._2 >= secondEPStartID)
      res.mapPartitionsWithIndex { case (i, rows) => Iterator(("partion-" + i, rows.size)) }.sortBy(_._2, false)
        .take(5).foreach(part => log.info("finalP1P2Distribution:" + part))
      res = res.distinct()
      res.mapPartitionsWithIndex { case (i, rows) => Iterator(("partion-" + i, rows.size)) }.sortBy(_._2, false)
        .take(5).foreach(part => log.info("finalP1P2DistributionDistinct:" + part))
      res
    }

    def trimDownProfiles(sourceId: Int, dataSet: DataSetConfiguration, profiles: RDD[Profile], profilePairs: RDD[(Int, Int, Double)]): RDD[Profile] = {
      val ids = profilePairs.map(idPair => if (sourceId == 0) {
        idPair._1
      } else {
        idPair._2
      }).distinct()
      ids.mapPartitionsWithIndex { case (i, rows) => Iterator(("partion-" + i, rows.size)) }.sortBy(_._2, false)
        .take(5).foreach(part => log.info("idsDistribution:" + part))
      val broadcastIds = profiles.sparkContext.broadcast(ids.collect)
      profiles.filter(_.sourceId == sourceId)
        .filter(p => broadcastIds.value.contains(p.id))
        .map(p => Profile(p.id, p.attributes.filter(y => y.key == dataSet.idField || dataSet.additionalAttrs.contains(y.key)), p.originalID, p.sourceId))
    }

    val finalP1P2Map = trimThenSwap(matchedPairsWithSimilarity, secondEPStartID)
    finalP1P2Map.persist(StorageLevel.MEMORY_AND_DISK)
    val (trimDownProfile1, trimDownProfile2) = (
      trimDownProfiles(0, dataSet1, profiles1, finalP1P2Map),
      trimDownProfiles(1, dataSet2, profiles2, finalP1P2Map)
    )
    val p1B = finalP1P2Map.sparkContext.broadcast(trimDownProfile1.collect)
    val p2B = finalP1P2Map.sparkContext.broadcast(trimDownProfile2.collect)
    val rows: RDD[Row] = resolveRowsV2(finalP1P2Map, idFieldProvided, dataSet1, dataSet2, p1B, p2B, secondEPStartID, showSimilarity)
    finalP1P2Map.unpersist()
    (columnNames, rows)
  }

  private def trimDownByIdWithSimilarity(sourceId: Int, dataSet: DataSetConfiguration, profiles: RDD[Profile], profilePairMap: RDD[(String, String, Double)], idFieldsProvided: Boolean) = {
    trimDownById(sourceId, dataSet, profiles, profilePairMap.map(x => (x._1, x._2)), idFieldsProvided)
  }

  private def trimDownById(sourceId: Int, dataSet: DataSetConfiguration, profiles: RDD[Profile], profileMatchPair: RDD[(String, String)], idFieldsProvided: Boolean) = {
    val idSet = profileMatchPair.map(x => if (sourceId == 0) {
      x._1
    } else {
      x._2
    }).distinct()
    val idSetB = profiles.sparkContext.broadcast(idSet.collect()) //long-running over 2 hours
    if (debug) {
      log.info("trimDownByProfileId - sourceId=" + sourceId + ",idSet=" + idSetB.value)
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
      val profilePairMap = profileMapAsIdMapWithSimilarity(profileMatches, idFieldsProvided)
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
      val profilePairMap = profileMapAsIdMap(profileMatches, idFieldsProvided)
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

  /**
   * resolve the row data by loaded profile id instead of original profile id
   *
   * @param finalMap
   * @param idFieldsProvided
   * @param dataSet1
   * @param dataSet2
   * @param p1B
   * @param p2B
   * @param secondEPStartID
   */
  def resolveRowsV2(finalMap: RDD[(Int, Int, Double)],
                    idFieldsProvided: Boolean,
                    dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration,
                    p1B: Broadcast[Array[Profile]], p2B: Broadcast[Array[Profile]],
                    secondEPStartID: Int,
                    showSimilarity: Boolean = false): RDD[Row] = {
    log.info("resolveRowsV2 - showSimilarity=" + showSimilarity + ", idFieldsProvided=" + idFieldsProvided)
    val rows = finalMap.map(x => {
      def getProfile(broadcastProfiles: Broadcast[Array[Profile]], profileId: String): Profile = {
        broadcastProfiles.value.find(p => p.id.toString == profileId).get
      }

      def getId(p: Profile, baseId: Int = 0): String = if (idFieldsProvided) {
        p.originalID
      } else {
        (p.id - baseId).toString
      }

      def retrieveAdditionalAttr(p: Profile, dataSetConf: DataSetConfiguration): Seq[String] = {
        dataSetConf.additionalAttrs.map(ma => p.attributes.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value)
      }

      val profile1 = getProfile(p1B, x._1.toString)
      val profile2 = getProfile(p2B, x._2.toString)
      var entry = Seq[String]()
      if (showSimilarity) {
        entry :+= x._3.toString
      }
      entry :+= getId(profile1)
      entry ++= retrieveAdditionalAttr(profile1, dataSet1)
      entry :+= getId(profile2, secondEPStartID)
      entry ++= retrieveAdditionalAttr(profile2, dataSet2)
      Row.fromSeq(entry)
    })
    unpersist(p1B, p2B)
    rows
  }

  def adjustProfile2RelativeId(profile2Id: String, secondEPStartID: Int, idFieldsProvided: Boolean): String = {
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
    val matchedPairsById = matchedPairs.keyBy(_._1) //over 2.4hour on 9m data
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


  def resolveColumns(moreAttr1s: Seq[String], moreAttr2s: Seq[String], showSimilarity: Boolean = false) = {
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
