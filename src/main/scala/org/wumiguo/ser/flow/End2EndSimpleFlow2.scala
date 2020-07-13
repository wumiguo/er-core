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
object End2EndSimpleFlow2 extends ERFlow with SparkEnvSetup {
  //val log = LoggerFactory.getLogger(getClass.getName)

  override def run(args: Array[String]): Unit = {
    //data reading
    val spark = createLocalSparkSession(getClass.getName)
    log.info("launch full end2end flow")
    val gtPath = getClass.getClassLoader.getResource("sampledata/dblpAcmIdDuplicates.gen.csv").getPath
    log.info("load ground-truth from path {}", gtPath)
    val gtRdd = CSVLoader.loadGroundTruth(gtPath)
    log.info("gt size is {}", gtRdd.count())
    val sourceId1 = 1001
    val sourceId2 = 1002
    val ep1Path = getClass.getClassLoader.getResource("sampledata/acmProfiles.gen.csv").getPath
    val ep1Rdd = CSVLoader.loadProfilesAdvanceMode(ep1Path, startIDFrom = 0, separator = ",", header = true, sourceId = sourceId1)
    log.info("ep1 size is {}", ep1Rdd.count())
    val ep2Path = getClass.getClassLoader.getResource("sampledata/dblpProfiles.gen.csv").getPath
    val ep2Rdd = CSVLoader.loadProfilesAdvanceMode(ep2Path, startIDFrom = 0, separator = ",", header = true, sourceId = sourceId2)
    log.info("ep2 size is {}", ep2Rdd.count())
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
