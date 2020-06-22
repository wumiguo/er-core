package org.wumiguo.ser.entity.parameter

import scala.beans.BeanProperty

class DatasetConfig(@BeanProperty var path: String, @BeanProperty var format: String, @BeanProperty var dataSetId: String, @BeanProperty var attribute: Array[String]) {

}
