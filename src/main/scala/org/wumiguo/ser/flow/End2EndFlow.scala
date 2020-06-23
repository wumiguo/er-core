package org.wumiguo.ser.flow

import org.slf4j.LoggerFactory
import org.wumiguo.ser.ERFlowLauncher.getClass
import org.wumiguo.ser.common.SparkEnvSetup
import org.wumiguo.ser.dataloader.CSVLoader
import org.wumiguo.ser.methods.datastructure.Profile
import org.wumiguo.ser.methods.entitymatching.{EntityMatching, MatchingFunctions}

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
    val ep1Rdd = CSVLoader.loadProfiles2(ep1Path, startIDFrom = 0, separator = ",", header = true)
    log.info("ep1 size is {}", ep1Rdd.count())
    val ep2Path = getClass.getClassLoader.getResource("sampledata/dblpProfiles.gen.csv").getPath
    val ep2Rdd = CSVLoader.loadProfiles2(ep2Path, startIDFrom = 0, separator = ",", header = true)
    log.info("ep2 size is {}", ep2Rdd.count())
    //build blocks

    //block cleaning

    //comparision cleaning

    //entity matching

    //entity clustering
    val p1 = Profile(1)
    val p2 = Profile(2)
    EntityMatching.profileMatching(p1, p2, MatchingFunctions.chfCosineSimilarity)
  }
}
