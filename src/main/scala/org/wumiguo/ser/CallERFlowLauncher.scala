package org.wumiguo.ser

/**
 * @author levinliu
 *         Created on 2020/7/17
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object CallERFlowLauncher {
  def main(args: Array[String]): Unit = {
    var flowArgs = Array[String]()
    flowArgs +:= "flowType=SSBatchV2Join"
    //    flowArgs +:= "flowType=SSParaJoin"
    flowArgs +:= "dataSet1=" + "src/main/resources/sampledata/dt01.csv"
    flowArgs +:= "dataSet1-id=" + "t_id"
    flowArgs +:= "dataSet1-format=" + "csv"
    flowArgs +:= "dataSet1-attrSet=" + "t_pid,system_id"
    flowArgs +:= "dataSet1-filterSize=2"
    flowArgs +:= "dataSet1-filter0=site:CN"
    flowArgs +:= "dataSet1-filter1=t_date:20200715,20200719"
    flowArgs +:= "dataSet1-additionalAttrSet=t_user,site,t_date,system_id,t_pid"
    flowArgs +:= "dataSet2=" + "src/main/resources/sampledata/dp01.csv"
    flowArgs +:= "dataSet2-id=" + "p_id"
    flowArgs +:= "dataSet2-format=" + "csv"
    flowArgs +:= "dataSet2-attrSet=" + "p_id,system_id"
    flowArgs +:= "dataSet2-filterSize=1"
    flowArgs +:= "dataSet2-filter0=type:fund"
    flowArgs +:= "dataSet2-additionalAttrSet=p_name,remark,system_id"
    flowArgs +:= "joinFieldsWeight=0.001,0.999"
    flowArgs +:= "optionSize=4"
    flowArgs +:= "option0=q:2"
    flowArgs +:= "option1=threshold:1" //0,1,2
    flowArgs +:= "option2=algorithm:EDJoin"
    flowArgs +:= "option3=relativeLinkageThreshold:0.000001"
    flowArgs +:= "outputPath=" + "output/trade-product"
    flowArgs +:= "outputType=" + "csv"
    flowArgs +:= "joinResultFile=" + "tp_join_v2"
    flowArgs +:= "overwriteOnExist=" + "true"
    flowArgs +:= "showSimilarity=" + "true"
    ERFlowLauncher.main(flowArgs)
  }
}
