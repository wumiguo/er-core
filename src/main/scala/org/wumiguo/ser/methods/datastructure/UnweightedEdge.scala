package org.wumiguo.ser.methods.datastructure

/**
 * Represents an unweighted and undirected edge between two profiles
 * If it used in a clean-clean dataset the firstProfileID refers to entity profile in first dataset,
 * and the second one to the entity profile in second dataset
 *
 *
 * @author levinliu
 * @since 2020/06/22
 */
case class UnweightedEdge(firstProfileID : Int, secondProfileID : Int) extends EdgeTrait with Serializable{

}
