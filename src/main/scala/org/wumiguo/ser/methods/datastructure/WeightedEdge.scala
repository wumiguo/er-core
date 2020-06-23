package org.wumiguo.ser.methods.datastructure

/**
 * Represents a weighted and undirected edge between two profiles
 * If it used in a clean-clean dataset the firstProfileID refers to entity profile in first dataset,
 * and the second one to the entity profile in second dataset
 *
 * Note: set weight=-1.0 when the 2 ep entry have nothing in common, that's, the jaccard similarity value as 0.0
 *
 * @author levinliu
 * @since 2020/06/22
 */
case class WeightedEdge(firstProfileID: Int, secondProfileID: Int, weight: Double) extends EdgeTrait with Serializable {

}
