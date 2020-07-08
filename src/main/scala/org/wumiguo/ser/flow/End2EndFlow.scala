package org.wumiguo.ser.flow

import org.slf4j.LoggerFactory
import org.wumiguo.ser.ERFlowLauncher.getClass
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.dataloader.CSVLoader
import org.wumiguo.ser.flow.End2EndFlow.log
import org.wumiguo.ser.methods.blockbuilding.TokenBlocking
import org.wumiguo.ser.methods.blockrefinement.{BlockFiltering, BlockPurging}
import org.wumiguo.ser.methods.datastructure.{KeysCluster, Profile}
import org.wumiguo.ser.methods.entityclustering.EntityClusterUtils
import org.wumiguo.ser.methods.entitymatching.{EntityMatching, MatchingFunctions}
import org.wumiguo.ser.methods.util.Converters

/**
 * @author levinliu
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object End2EndFlow extends ERFlow with SparkEnvSetup {
  //val log = LoggerFactory.getLogger(getClass.getName)

  def run: Unit = {
    //data reading
    val sparkSession = createLocalSparkSession(getClass.getName)
    log.info("launch full end2end flow")
    val gtPath = getClass.getClassLoader.getResource("sampledata/dblpAcmIdDuplicates.gen.csv").getPath
    log.info("load ground-truth from path {}", gtPath)
    val gtRdd = CSVLoader.loadGroundTruth(gtPath)
    log.info("gt size is {}", gtRdd.count())
    val ep1Path = getClass.getClassLoader.getResource("sampledata/acmProfiles.gen.csv").getPath
    val ep1Rdd = CSVLoader.loadProfiles2(ep1Path, startIDFrom = 0, separator = ",", header = true, sourceId = 1001)
    log.info("ep1 size is {}", ep1Rdd.count())
    val ep2Path = getClass.getClassLoader.getResource("sampledata/dblpProfiles.gen.csv").getPath
    val ep2Rdd = CSVLoader.loadProfiles2(ep2Path, startIDFrom = 0, separator = ",", header = true, sourceId = 2002)
    log.info("ep2 size is {}", ep2Rdd.count())
    //build blocks
    val separators = Array[Int]()
    var clusters = List[KeysCluster]()
    //TODO: generate the key clusters programmatically
    clusters = KeysCluster(100111, List("1001_year", "2002_year")) :: clusters
    clusters = KeysCluster(100112, List("1001_title", "2002_title")) :: clusters
    clusters = KeysCluster(100113, List("1001_authors", "2002_authors")) :: clusters
    clusters = KeysCluster(100114, List("1001_venue", "2002_venue")) :: clusters
    //    TokenBlocking.createBlocksCluster()
    val ep1Blocks = TokenBlocking.createBlocksCluster(ep1Rdd, separators, clusters)
    val ep2Blocks = TokenBlocking.createBlocksCluster(ep2Rdd, separators, clusters)
    log.info("ep1blocks {}", ep1Blocks.count())
    log.info("ep2blocks {}", ep2Blocks.count())
    ep1Blocks.top(10).foreach(b => log.info("ep1b is {}", b))
    ep2Blocks.top(10).foreach(b => log.info("ep2b is {}", b))
    val profileBlocks1 = Converters.blocksToProfileBlocks(ep1Blocks)
    val profileBlocks2 = Converters.blocksToProfileBlocks(ep2Blocks)
    log.info("pb count1 {}", profileBlocks1.count())
    log.info("pb count2 {}", profileBlocks2.count())
    log.info("pb first1 {}", profileBlocks1.first())
    log.info("pb first2 {}", profileBlocks2.first())
    //block cleaning
    val profileBlockFilter1 = BlockFiltering.blockFiltering(profileBlocks1, r = 0.5)
    val profileBlockFilter2 = BlockFiltering.blockFiltering(profileBlocks2, r = 0.5)
    log.info("pb count1 {}", profileBlockFilter1.count())
    log.info("pb count2 {}", profileBlockFilter2.count())
    log.info("pb first1 {}", profileBlockFilter1.first())
    log.info("pb first2 {}", profileBlockFilter2.first())
    //comparision cleaning
    val abRdd1 = Converters.profilesBlockToBlocks(profileBlockFilter1)
    val abRdd2 = Converters.profilesBlockToBlocks(profileBlockFilter2)
    val pAbRdd1 = BlockPurging.blockPurging(abRdd1, 0.6)
    val pAbRdd2 = BlockPurging.blockPurging(abRdd2, 0.6)
    //entity matching
    val weRdd = ep1Rdd.flatMap(p1 =>
      ep2Rdd.map(p2 =>
        EntityMatching.profileMatching(p1, p2, MatchingFunctions.jaccardSimilarity)
      ).toLocalIterator
    )
    //entity clustering
    val connected = EntityClusterUtils.connectedComponents(weRdd)
    connected.top(10).foreach(x => println("connected=" + x))
    val p1 = Profile(1)
    val p2 = Profile(2)
    EntityMatching.profileMatching(p1, p2, MatchingFunctions.chfCosineSimilarity)
  }
}
