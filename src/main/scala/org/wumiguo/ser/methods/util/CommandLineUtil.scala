package org.wumiguo.ser.methods.util

object CommandLineUtil {

  def getParameter(args: Array[String], name: String, defaultValue: String = null): String = {
    val parameterPair = args.find(_.startsWith(name + "=")).orNull
    if (null != parameterPair) {
      val p = parameterPair.split("=")
      if (p.size > 1) {
        p(1)
      } else {
        ""
      }
    } else {
      defaultValue
    }
  }

}
