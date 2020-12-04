package org.wumiguo.ser.flow.configuration

import org.slf4j.LoggerFactory
import org.wumiguo.ser.dataloader.ColumnBaseIndexLoader
import org.wumiguo.ser.methods.datastructure.KeyValue
import org.wumiguo.ser.methods.util.CommandLineUtil.getParameter

/**
 * @author levinliu
 *         Created on 2020/9/10
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object CommandLineConfigLoader {
  val log = LoggerFactory.getLogger(this.getClass.getName)

  def load(args: Array[String], dataSetPrefix: String): DataSetConfiguration = {
    loadIndexBase(args, dataSetPrefix)
  }

  def loadFieldNameExplicitly(args: Array[String], dataSetPrefix: String): DataSetConfiguration = {
    def splitAttr(attrs: String) = if (attrs == "") Array[String]() else attrs.split(",")

    val path = getParameter(args, dataSetPrefix, "")
    val idField = getParameter(args, dataSetPrefix + "-id", "")
    val attrs = getParameter(args, dataSetPrefix + "-attrSet", "")
    val joinAttrs = splitAttr(attrs)
    val moreAttrsToExtract = getParameter(args, dataSetPrefix + "-additionalAttrSet", "")
    val moreAttrs = splitAttr(moreAttrsToExtract)
    val filterOptions = FilterOptions.getOptions(dataSetPrefix, args).toArray
    DataSetConfiguration(path, idField, joinAttrs, moreAttrs, filterOptions)
  }

  def loadIndexBase(args: Array[String], dataSetPrefix: String): DataSetConfiguration = {
    val dataSet = loadFieldNameExplicitly(args, dataSetPrefix)
    log.info("dataSetConf=" + dataSet)
    var joinAttrs = dataSet.joinAttrs
    val indexBaseFields = !joinAttrs.isEmpty && joinAttrs.count(_.startsWith("_")) == joinAttrs.size
    if (!indexBaseFields) {
      dataSet
    } else {
      val columnIndexes = ColumnBaseIndexLoader.loadIndexMap(dataSet.path)
      var idField = dataSet.idField
      joinAttrs = joinAttrs.map(x => columnIndexes(x.substring(1).toInt - 1))
      if (!idField.startsWith("_")) {
        throw new RuntimeException("Given id field should be index-based pattern, e.g. '_1' invalidIdField=" + idField)
      }

      idField = columnIndexes(idField.substring(1).toInt - 1)
      var moreAttrs = dataSet.additionalAttrs
      if (!moreAttrs.isEmpty && moreAttrs.count(_.startsWith("_")) != moreAttrs.size) {
        throw new RuntimeException("Given additional attributes should be all index-based pattern, e.g. '_1', invalidAdditionalAttributes=" + moreAttrs)
      }
      moreAttrs = moreAttrs.map(x => columnIndexes(x.substring(1).toInt - 1))
      var filterOptions = dataSet.filterOptions
      if (!filterOptions.isEmpty && filterOptions.count(_.key.startsWith("_")) != filterOptions.size) {
        throw new RuntimeException("Given filter options should be all startWith index-based pattern, e.g. '_1', invalidFilterOptions=" + filterOptions)
      }
      filterOptions = filterOptions.map(x => KeyValue(columnIndexes(x.key.substring(1).toInt - 1), x.value))
      val mappedDataSet = DataSetConfiguration(dataSet.path, idField, joinAttrs, moreAttrs, filterOptions)
      log.info("mappedDataSet=" + mappedDataSet)
      mappedDataSet
    }
  }
}
