package org.wumiguo.ser.methods.similarityjoins.datastructure

/**
 * @author levinliu
 * Created on 2020/6/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
case class PrefixEntry(docId: Int, tokenPos: Int, docLen: Int) extends Ordered[PrefixEntry]{

  override def compare(that: PrefixEntry): Int = {
    this.docLen.compare(that.docLen)
  }
}
