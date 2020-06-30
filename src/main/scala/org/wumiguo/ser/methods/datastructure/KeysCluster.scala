package org.wumiguo.ser.methods.datastructure

/**
 * Represents a cluster of keys.
 *
 * E.g. when we have 2 entity profiles input having columns
 * epI1 (title,name,date) from data source 001
 * epI2 (title,author,publishingDate) from data source 002
 * then we can create a KeysCluster as
 *  (1,(001_title,002_title)) // which means title in epI1 are logically matching with title in epI2
 *  (2,(001_name,002_author)) // which means name in epI1 are logically matching with author in epI2
 *
 * @author levinliu
 *
 * @param id
 * @param keys
 * @param entropy
 * @param filtering
 */
case class KeysCluster(id : Int, keys: List[String], entropy : Double = 1, filtering : Double = 0.8){}
