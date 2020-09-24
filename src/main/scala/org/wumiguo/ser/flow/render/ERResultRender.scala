package org.wumiguo.ser.flow.render

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory
import org.wumiguo.ser.dataloader.{DataTypeResolver, ProfileLoaderFactory, ProfileLoaderTrait}
import org.wumiguo.ser.dataloader.filter.SpecificFieldValueFilter
import org.wumiguo.ser.entity.parameter.DataSetConfig
import org.wumiguo.ser.flow.configuration.DataSetConfiguration
import org.wumiguo.ser.methods.datastructure.{KeyValue, Profile}

/**
 * @author levinliu
 *         Created on 2020/9/8
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object ERResultRender extends Serializable{
  val log = LoggerFactory.getLogger(this.getClass.getName)

  def renderResultV2(dataSet1: DataSetConfiguration, dataSet2: DataSetConfiguration, secondEPStartID: Int,
                   matchDetails: RDD[(Int, Int, Double)], profiles: RDD[Profile], matchedPairs: RDD[(Int, Int,Double)],
                   showSimilarity: Boolean): (Seq[String], RDD[Row]) = {
    log.info("showSimilarity=" + showSimilarity)
    if (showSimilarity) {
      log.info("matchedPairsWithSimilarityCount=" + matchedPairs.count())
      val profileMatches2 = mapMatchesWithProfilesAndSimilarity(matchedPairs, profiles, secondEPStartID)
      log.info("profileMatchesCount=" + profileMatches2.count())
      val matchesInDiffDataSet2 = profileMatches2.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
      log.info("matchesInDiffDataSet2 size =" + matchesInDiffDataSet2.count())
      val finalMap2 = matchesInDiffDataSet2.map(x => (x._1._1.originalID, x._1._2.originalID, x._1._3))
      log.info("finalmap2 size =" + finalMap2.count())

      val p1IDFilterOption = finalMap2.map(x => KeyValue(dataSet1.idField, x._1)).toLocalIterator.toList
      val finalProfiles1 = getProfileLoader(dataSet1.path).load(dataSet1.path, realIDField = dataSet1.idField,
        startIDFrom = 0, sourceId = 0, keepRealID = dataSet1.includeRealID, fieldsToKeep = dataSet1.additionalAttrs.toList,
        fieldValuesScope = p1IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val p2IDFilterOption = finalMap2.map(x => KeyValue(dataSet2.idField, x._2)).toLocalIterator.toList
      val finalProfiles2 = getProfileLoader(dataSet2.path).load(dataSet2.path, realIDField = dataSet2.idField,
        startIDFrom = 0, sourceId = 1, keepRealID = dataSet2.includeRealID, fieldsToKeep = dataSet2.additionalAttrs.toList,
        fieldValuesScope = p2IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      if (!finalProfiles1.isEmpty()) {
        finalProfiles1.take(3).foreach(x => log.info("fp1=" + x))
      }
      if (!finalProfiles2.isEmpty()) {
        finalProfiles2.take(3).foreach(x => log.info("fp2=" + x))
      }
      log.info("fp1count=" + finalProfiles1.count())
      log.info("fp2count=" + finalProfiles2.count())
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
      val rows = finalMap2.map(x => {
        var entry = Seq[String]()
        entry :+= x._3.toString
        entry :+= x._1
        val attr1 = p1B.value.filter(p => p.originalID == x._1).flatMap(p => p.attributes)
        entry ++= dataSet1.additionalAttrs.map(ma => attr1.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        entry :+= x._2
        val attr2 = p2B.value.filter(p => p.originalID == x._2).flatMap(p => p.attributes)
        entry ++= dataSet2.additionalAttrs.map(ma => attr2.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        Row.fromSeq(entry)
      })
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
      log.info("moreAttr1s=" + dataSet1.additionalAttrs.toList)
      log.info("moreAttr2s=" + dataSet2.additionalAttrs.toList)
      log.info("matchesInDiffDataSet1 size =" + matchesInDiffDataSet.count())
      val finalMap = matchesInDiffDataSet.map(x => (x._1._1.originalID, x._1._2.originalID))
      log.info("finalmap1 size =" + finalMap.count())

      val p1IDFilterOption = finalMap.map(x => KeyValue(dataSet1.idField, x._1)).toLocalIterator.toList
      val finalProfiles1 = getProfileLoader(dataSet1.path).load(dataSet1.path, realIDField = dataSet1.idField,
        startIDFrom = 0, sourceId = 0, keepRealID = dataSet1.includeRealID, fieldsToKeep = dataSet1.additionalAttrs.toList,
        fieldValuesScope = p1IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val p2IDFilterOption = finalMap.map(x => KeyValue(dataSet2.idField, x._2)).toLocalIterator.toList
      val finalProfiles2 = getProfileLoader(dataSet2.path).load(dataSet2.path, realIDField = dataSet2.idField,
        startIDFrom = 0, sourceId = 0, keepRealID = dataSet2.includeRealID, fieldsToKeep = dataSet2.additionalAttrs.toList,
        fieldValuesScope = p2IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      if (!finalProfiles1.isEmpty()) {
        finalProfiles1.take(3).foreach(x => log.info("fp1=" + x))
      }
      if (!finalProfiles2.isEmpty()) {
        finalProfiles2.take(3).foreach(x => log.info("fp2=" + x))
      }
      log.info("fp1count=" + finalProfiles1.count())
      log.info("fp2count=" + finalProfiles2.count())
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
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
      (columnNames, rows)
    }
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
      val matchesInDiffDataSet2 = profileMatches2.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
      log.info("matchesInDiffDataSet2 size =" + matchesInDiffDataSet2.count())
      val finalMap2 = matchesInDiffDataSet2.map(x => (x._1._1.originalID, x._1._2.originalID, x._1._3))
      log.info("finalmap2 size =" + finalMap2.count())

      val p1IDFilterOption = finalMap2.map(x => KeyValue(dataSet1.idField, x._1)).toLocalIterator.toList
      val finalProfiles1 = getProfileLoader(dataSet1.path).load(dataSet1.path, realIDField = dataSet1.idField,
        startIDFrom = 0, sourceId = 0, keepRealID = dataSet1.includeRealID, fieldsToKeep = dataSet1.additionalAttrs.toList,
        fieldValuesScope = p1IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val p2IDFilterOption = finalMap2.map(x => KeyValue(dataSet2.idField, x._2)).toLocalIterator.toList
      val finalProfiles2 = getProfileLoader(dataSet2.path).load(dataSet2.path, realIDField = dataSet2.idField,
        startIDFrom = 0, sourceId = 1, keepRealID = dataSet2.includeRealID, fieldsToKeep = dataSet2.additionalAttrs.toList,
        fieldValuesScope = p2IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      if (!finalProfiles1.isEmpty()) {
        finalProfiles1.take(3).foreach(x => log.info("fp1=" + x))
      }
      if (!finalProfiles2.isEmpty()) {
        finalProfiles2.take(3).foreach(x => log.info("fp2=" + x))
      }
      log.info("fp1count=" + finalProfiles1.count())
      log.info("fp2count=" + finalProfiles2.count())
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
      val rows = finalMap2.map(x => {
        var entry = Seq[String]()
        entry :+= x._3.toString
        entry :+= x._1
        val attr1 = p1B.value.filter(p => p.originalID == x._1).flatMap(p => p.attributes)
        entry ++= dataSet1.additionalAttrs.map(ma => attr1.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        entry :+= x._2
        val attr2 = p2B.value.filter(p => p.originalID == x._2).flatMap(p => p.attributes)
        entry ++= dataSet2.additionalAttrs.map(ma => attr2.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        Row.fromSeq(entry)
      })
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
      log.info("moreAttr1s=" + dataSet1.additionalAttrs.toList)
      log.info("moreAttr2s=" + dataSet2.additionalAttrs.toList)
      log.info("matchesInDiffDataSet1 size =" + matchesInDiffDataSet.count())
      val finalMap = matchesInDiffDataSet.map(x => (x._1._1.originalID, x._1._2.originalID))
      log.info("finalmap1 size =" + finalMap.count())

      val p1IDFilterOption = finalMap.map(x => KeyValue(dataSet1.idField, x._1)).toLocalIterator.toList
      val finalProfiles1 = getProfileLoader(dataSet1.path).load(dataSet1.path, realIDField = dataSet1.idField,
        startIDFrom = 0, sourceId = 0, keepRealID = dataSet1.includeRealID, fieldsToKeep = dataSet1.additionalAttrs.toList,
        fieldValuesScope = p1IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val p2IDFilterOption = finalMap.map(x => KeyValue(dataSet2.idField, x._2)).toLocalIterator.toList
      val finalProfiles2 = getProfileLoader(dataSet2.path).load(dataSet2.path, realIDField = dataSet2.idField,
        startIDFrom = 0, sourceId = 0, keepRealID = dataSet2.includeRealID, fieldsToKeep = dataSet2.additionalAttrs.toList,
        fieldValuesScope = p2IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      if (!finalProfiles1.isEmpty()) {
        finalProfiles1.take(3).foreach(x => log.info("fp1=" + x))
      }
      if (!finalProfiles2.isEmpty()) {
        finalProfiles2.take(3).foreach(x => log.info("fp2=" + x))
      }
      log.info("fp1count=" + finalProfiles1.count())
      log.info("fp2count=" + finalProfiles2.count())
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      val columnNames: Seq[String] = resolveColumns(dataSet1.additionalAttrs, dataSet2.additionalAttrs, showSimilarity)
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
      (columnNames, rows)
    }
  }

  def renderResult(dataSet1IdField: String, moreAttr1s: Array[String], moreAttr2s: Array[String],
                   dataSet2IdField: String, dataSet1: DataSetConfig, dataSet2: DataSetConfig,
                   keepReadID1: Boolean, keepReadID2: Boolean, secondEPStartID: Int,
                   matchDetails: RDD[(Int, Int, Double)], profiles: RDD[Profile], matchedPairs: RDD[(Int, Int)],
                   showSimilarity: Boolean): (Seq[String], RDD[Row]) = {
    log.info("showSimilarity=" + showSimilarity)
    if (showSimilarity) {
      val matchedPairsWithSimilarity = enrichPairs(matchedPairs)
      log.info("matchedPairsWithSimilarityCount=" + matchedPairsWithSimilarity.count())
      val profileMatches2 = mapMatchesWithProfilesAndSimilarity(matchedPairsWithSimilarity, profiles, secondEPStartID)
      log.info("profileMatchesCount=" + profileMatches2.count())
      val matchesInDiffDataSet2 = profileMatches2.filter(t => t._1.sourceId != t._2.sourceId).zipWithIndex()
      log.info("matchesInDiffDataSet2 size =" + matchesInDiffDataSet2.count())
      val finalMap2 = matchesInDiffDataSet2.map(x => (x._1._1.originalID, x._1._2.originalID, x._1._3))
      log.info("finalmap2 size =" + finalMap2.count())

      val p1IDFilterOption = finalMap2.map(x => KeyValue(dataSet1IdField, x._1)).toLocalIterator.toList
      val finalProfiles1 = getProfileLoader(dataSet1.path).load(dataSet1.path, realIDField = dataSet1.dataSetId,
        startIDFrom = 0, sourceId = 0, keepRealID = keepReadID1, fieldsToKeep = moreAttr1s.toList,
        fieldValuesScope = p1IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val p2IDFilterOption = finalMap2.map(x => KeyValue(dataSet2IdField, x._2)).toLocalIterator.toList
      val finalProfiles2 = getProfileLoader(dataSet2.path).load(dataSet2.path, realIDField = dataSet2.dataSetId,
        startIDFrom = 0, sourceId = 1, keepRealID = keepReadID2, fieldsToKeep = moreAttr2s.toList,
        fieldValuesScope = p2IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      if (!finalProfiles1.isEmpty()) {
        finalProfiles1.take(3).foreach(x => log.info("fp1=" + x))
      }
      if (!finalProfiles2.isEmpty()) {
        finalProfiles2.take(3).foreach(x => log.info("fp2=" + x))
      }
      log.info("fp1count=" + finalProfiles1.count())
      log.info("fp2count=" + finalProfiles2.count())
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      val columnNames: Seq[String] = resolveColumns(moreAttr1s, moreAttr2s, showSimilarity)
      val rows = finalMap2.map(x => {
        var entry = Seq[String]()
        entry :+= x._3.toString
        entry :+= x._1
        val attr1 = p1B.value.filter(p => p.originalID == x._1).flatMap(p => p.attributes)
        entry ++= moreAttr1s.map(ma => attr1.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        entry :+= x._2
        val attr2 = p2B.value.filter(p => p.originalID == x._2).flatMap(p => p.attributes)
        entry ++= moreAttr2s.map(ma => attr2.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        Row.fromSeq(entry)
      })
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
      log.info("moreAttr1s=" + moreAttr1s.toList)
      log.info("moreAttr2s=" + moreAttr2s.toList)
      log.info("matchesInDiffDataSet1 size =" + matchesInDiffDataSet.count())
      val finalMap = matchesInDiffDataSet.map(x => (x._1._1.originalID, x._1._2.originalID))
      log.info("finalmap1 size =" + finalMap.count())

      val p1IDFilterOption = finalMap.map(x => KeyValue(dataSet1IdField, x._1)).toLocalIterator.toList
      val finalProfiles1 = getProfileLoader(dataSet1.path).load(dataSet1.path, realIDField = dataSet1.dataSetId,
        startIDFrom = 0, sourceId = 0, keepRealID = keepReadID1, fieldsToKeep = moreAttr1s.toList,
        fieldValuesScope = p1IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      val p1B = matchDetails.sparkContext.broadcast(finalProfiles1.collect())
      val p2IDFilterOption = finalMap.map(x => KeyValue(dataSet2IdField, x._2)).toLocalIterator.toList
      val finalProfiles2 = getProfileLoader(dataSet2.path).load(dataSet2.path, realIDField = dataSet2.dataSetId,
        startIDFrom = 0, sourceId = 0, keepRealID = keepReadID2, fieldsToKeep = moreAttr2s.toList,
        fieldValuesScope = p2IDFilterOption,
        filter = SpecificFieldValueFilter
      )
      if (!finalProfiles1.isEmpty()) {
        finalProfiles1.take(3).foreach(x => log.info("fp1=" + x))
      }
      if (!finalProfiles2.isEmpty()) {
        finalProfiles2.take(3).foreach(x => log.info("fp2=" + x))
      }
      log.info("fp1count=" + finalProfiles1.count())
      log.info("fp2count=" + finalProfiles2.count())
      val p2B = matchDetails.sparkContext.broadcast(finalProfiles2.collect())
      val columnNames: Seq[String] = resolveColumns(moreAttr1s, moreAttr2s, showSimilarity)
      val rows = finalMap.map(x => {
        var entry = Seq[String]()
        entry :+= x._1
        val attr1 = p1B.value.filter(p => p.originalID == x._1).flatMap(p => p.attributes)
        entry ++= moreAttr1s.map(ma => attr1.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        entry :+= x._2
        val attr2 = p2B.value.filter(p => p.originalID == x._2).flatMap(p => p.attributes)
        entry ++= moreAttr2s.map(ma => attr2.find(_.key == ma).getOrElse(KeyValue("", "N/A")).value).toSeq
        Row.fromSeq(entry)
      })
      (columnNames, rows)
    }
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
