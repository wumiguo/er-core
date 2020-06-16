package org.wumiguo.ser.methods.datastructure

/**
 * @author levinliu
 * Created on 2020/6/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
case class VertexWeight(profileId: Int, weight: Double, numNeighbors: Int, connections: Map[Int, Double]) extends Ordered[VertexWeight] {
  override def compare(that: VertexWeight): Int = {
    val w1 = this.weight / this.numNeighbors
    val w2 = that.weight / that.numNeighbors
    val test = w1 - w2 + 0.00001 * this.connections.size.toDouble - 0.00001 * that.connections.size.toDouble
    if (test > 0) {
      -1
    }
    else if (test < 0) {
      1
    }
    else {
      0
    }
  }
}
