package org.wumiguo.ser.methods.util

object CommandLineUtil {

  def getParameter(args: Array[String], name: String, defaultValue: String = null): String = {
    val parameterPairs = args.filter(x => x.startsWith(name + "="))
    if (parameterPairs.length == 0) {
      defaultValue
    } else {
      val parameterPair = parameterPairs.last
      val kv = parameterPair.split("=")
      if (kv.size > 1) {
        kv(1)
      } else {
        ""
      }
    }
  }

}
