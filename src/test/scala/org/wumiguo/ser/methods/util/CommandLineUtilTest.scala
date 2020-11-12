package org.wumiguo.ser.methods.util

import org.scalatest.flatspec.AnyFlatSpec

/**
 * @author levinliu
 *         Created on 2020/11/12
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
class CommandLineUtilTest extends AnyFlatSpec {
  it should "getParameter" in {
    var flowArgs = Array[String]()
    flowArgs :+= "flowType=SSJoin"
    var value = CommandLineUtil.getParameter(flowArgs, "flowType", "N/A")
    assertResult("SSJoin")(value)
    flowArgs :+= "flowType=SSJoinPosL"
    value = CommandLineUtil.getParameter(flowArgs, "flowType", "N/A")
    assertResult("SSJoinPosL")(value)
  }
}
