package org.wumiguo.ser.entity.parameter

import scala.beans.BeanProperty

/**
 * Represent the original raw data set as flow input.
 * A dataSetConfig describe the path of source and also format, more importantly, the attributes to be collected from source.
 *
 * @param path
 * @param format
 * @param dataSetId
 * @param attributes
 */
class DataSetConfig(@BeanProperty var path: String, @BeanProperty var format: String, @BeanProperty var dataSetId: String, @BeanProperty var attributes: Array[String]) {

}
