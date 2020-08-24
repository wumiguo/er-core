package org.wumiguo.ser

/**
 * @author levinliu
 *         Created on 2020/7/17
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object CallERFlowLauncher {
  def main(args: Array[String]): Unit = {
    var flowArgs = Array[String]()
    flowArgs +:= "flowType=SSJoin"
    flowArgs +:= "dataSet1=" + "/Users/mac/Development/learn/er-job/data-er-input/trade.csv"
    flowArgs +:= "dataSet1-id=" + "t_id"
    flowArgs +:= "dataSet1-format=" + "csv"
    flowArgs +:= "dataSet1-attrSet=" + "t_pid"
    flowArgs +:= "dataSet1-filterSize=2"
    flowArgs +:= "dataSet1-filter0=site:CN"
    flowArgs +:= "dataSet1-filter1=t_date:20200715"
    flowArgs +:= "dataSet1-additionalAttrSet=t_user,site"
    flowArgs +:= "dataSet2=" + "/Users/mac/Development/learn/er-job/data-er-input/product.csv"
    flowArgs +:= "dataSet2-id=" + "p_id"
    flowArgs +:= "dataSet2-format=" + "csv"
    flowArgs +:= "dataSet2-attrSet=" + "p_id"
    flowArgs +:= "dataSet2-filterSize=1"
    flowArgs +:= "dataSet2-filter0=type:fund"
    flowArgs +:= "dataSet2-additionalAttrSet=p_name,remark"
    flowArgs +:= "joinFieldsWeight=0.8"
    flowArgs +:= "q=2"
    flowArgs +:= "optionSize=3"
    flowArgs +:= "option0=q:2"
    flowArgs +:= "option1=threshold:2" //0,1,2
    flowArgs +:= "option2=algorithm:EDJoin"
    flowArgs +:= "outputPath=" + "/Users/mac/Development/learn/er-spark/output/trade-product"
    flowArgs +:= "outputType=" + "csv"
    flowArgs +:= "joinResultFile=" + "aa"
    flowArgs +:= "overwriteOnExist=" + "true"
    ERFlowLauncher.main(flowArgs)
  }
}
