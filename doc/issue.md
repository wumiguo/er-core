##

## Common Issues
# Exception in thread "dag-scheduler-event-loop" java.lang.OutOfMemoryError: Metaspace
stop sbt and relaunch a new sbt REPL



//val combinedRdd = ep1Rdd.flatMap(p1 => (ep2Rdd.map(p2 => (p1, p2)).toLocalIterator))

2020-07-08 18:33:01 ERROR Executor:91 - Exception in task 0.0 in stage 50.0 (TID 30)
org.apache.spark.SparkException: This RDD lacks a SparkContext. It could happen in the following cases: 
(1) RDD transformations and actions are NOT invoked by the driver, but inside of other transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid because the values transformation and count action cannot be performed inside of the rdd1.map transformation. For more information, see SPARK-5063.
(2) When a Spark Streaming job recovers from checkpoint, this exception will be hit if a reference to an RDD not defined by the streaming job is used in DStream operations. For more information, See SPARK-13758.
	at org.apache.spark.rdd.RDD.org$apache$spark$rdd$RDD$$sc(RDD.scala:90)
	at org.apache.spark.rdd.RDD.withScope(RDD.scala:363)
	at org.apache.spark.rdd.RDD.map(RDD.scala:370)
