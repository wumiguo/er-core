package org.wumiguo.ser.flow

import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.dataloader.CSVLoader
import org.wumiguo.ser.methods.blockbuilding.TokenBlocking
import org.wumiguo.ser.methods.blockrefinement.{BlockFiltering, BlockPurging}
import org.wumiguo.ser.methods.datastructure.KeysCluster
import org.wumiguo.ser.methods.entityclustering.EntityClusterUtils
import org.wumiguo.ser.methods.entitymatching.{EntityMatching, MatchingFunctions}
import org.wumiguo.ser.methods.util.Converters

/**
 * @author levinliu
 *         Created on 2020/7/06
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object End2EndSimpleFlow3 extends ERFlow with SparkEnvSetup {
  //val log = LoggerFactory.getLogger(getClass.getName)

  def run(args: Array[String]) = {
    //data reading
    val spark = createLocalSparkSession(getClass.getName)
    log.info("launch full end2end flow")
    val gtPath = getClass.getClassLoader.getResource("sampledata/dblpAcmIdDuplicates.mini.gen.csv").getPath
    log.info("load ground-truth from path {}", gtPath)
    val gtRdd = CSVLoader.loadGroundTruth(gtPath)
    log.info("gt size is {}", gtRdd.count())
    val sourceId1 = 1001
    val sourceId2 = 1002
    val startIDFrom = 0
    val separator = ","
    val ep1Path = getClass.getClassLoader.getResource("sampledata/acmProfiles.mini.gen.csv").getPath
    val ep1Rdd = CSVLoader.loadProfilesAdvanceMode(ep1Path, startIDFrom, separator, header = true, sourceId = sourceId1)
    log.info("ep1 size is {}", ep1Rdd.count())
    val secondProfileStartIDFrom = ep1Rdd.count().toInt - 1 + startIDFrom
    val ep2Path = getClass.getClassLoader.getResource("sampledata/dblpProfiles.mini.gen.csv").getPath
    val ep2Rdd = CSVLoader.loadProfilesAdvanceMode(ep2Path, secondProfileStartIDFrom, separator, header = true, sourceId = sourceId2)
    log.info("ep2 size is {}", ep2Rdd.count())
    val allEPRdd = ep1Rdd.union(ep2Rdd)
    allEPRdd.sortBy(_.id).take(5).foreach(x => log.info("all-profileId=" + x))
    //build blocks
    val separators = Array[Int](secondProfileStartIDFrom)
    var clusters = List[KeysCluster]()
    //TODO: user pre-input to label the columns that potentially matched
    clusters = KeysCluster(100111, List(sourceId1 + "_year", sourceId2 + "_year")) :: clusters
    clusters = KeysCluster(100112, List(sourceId1 + "_title", sourceId2 + "_title")) :: clusters
    clusters = KeysCluster(100113, List(sourceId1 + "_authors", sourceId2 + "_authors")) :: clusters
    clusters = KeysCluster(100114, List(sourceId1 + "_venue", sourceId2 + "_venue")) :: clusters
    //    TokenBlocking.createBlocksCluster()
    val epBlocks = TokenBlocking.createBlocksCluster(allEPRdd, separators, clusters)
    log.info("pb-detail-bc count " + epBlocks.count() + " first " + epBlocks.first())
    epBlocks.top(5).foreach(b => log.info("ep1b is {}", b))
    val profileBlocks = Converters.blocksToProfileBlocks(epBlocks)
    log.info("pb-detail-bb count " + profileBlocks.count() + " first " + profileBlocks.first())
    //block cleaning
    val profileBlockFilter1 = BlockFiltering.blockFiltering(profileBlocks, r = 0.5)
    log.info("pb-detail-bf count " + profileBlockFilter1.count() + " first " + profileBlockFilter1.first())
    //comparision cleaning
    val abRdd1 = Converters.profilesBlockToBlocks(profileBlockFilter1)
    val pAbRdd1 = BlockPurging.blockPurging(abRdd1, 0.6)
    log.info("pb-detail-bp count " + pAbRdd1.count() + " first " + pAbRdd1.first())
    //entity matching
    val broadcastVar = spark.sparkContext.broadcast(ep1Rdd.collect())
    val combinedRdd = ep2Rdd.flatMap(p2 => broadcastVar.value.map(p1 => (p1, p2)))
    //val combinedRdd = ep1Rdd.flatMap(p1 => (ep2Rdd.map(p2 => (p1, p2)).toLocalIterator))
    combinedRdd.take(2).foreach(x => log.info("pb-detail-cb combined {}", x))
    val weRdd = combinedRdd.map(x => EntityMatching.profileMatching(x._1, x._2, MatchingFunctions.jaccardSimilarity))
    //entity clustering
    val connected = EntityClusterUtils.connectedComponents(weRdd)
    connected.top(5).foreach(x => log.info("pb-detail-cc connected=" + x))
    val all = connected.flatMap(x => x)
    val similarPairs = all.filter(x => x._3 > 0.6)
    import spark.implicits._
    val pwd = System.getProperty("user.dir")
    similarPairs.toDF.write.csv(pwd + "/output/" + System.currentTimeMillis() + "/data.csv")
    //val lessThan20 = connected.filter(x => x._3 < 0.2)
    spark.close()
  }

}
