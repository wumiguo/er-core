package org.wumiguo.ser.flow.configuration

import org.wumiguo.ser.methods.util.CommandLineUtil
import org.wumiguo.ser.methods.util.CommandLineUtil.getParameter

/**
 * @author levinliu
 *         Created on 2020/9/10
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object CommandLineConfigLoader {

  def load(args: Array[String], dataSetPrefix: String): DataSetConfiguration = {
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
}
