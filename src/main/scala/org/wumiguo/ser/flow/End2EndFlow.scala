package org.wumiguo.ser.flow

import org.slf4j.LoggerFactory
import org.wumiguo.ser.ERFlowLauncher.getClass
import org.wumiguo.ser.dataloader.CSVLoader

/**
 * @author levinliu
 *         Created on 2020/6/18
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object End2EndFlow extends ERFlow {
  val log = LoggerFactory.getLogger(getClass.getName)

  def run: Unit = {
    log.info("launch full end2end flow")
    val gtPath = getClass.getClassLoader.getResource("sampledata/dblpAcmIdDuplicates.gen.csv").getPath
    log.info("load ground-truth from path {}", gtPath)
    val gtRdd = CSVLoader.loadGroundTruth(gtPath)
    log.info("gt size is {}", gtRdd.count())

  }
}
