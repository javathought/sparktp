import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
val sc = new JavaSparkContext(sparkConf)

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

val vertices = List(
  (1l, ("lundi")),
  (2l, ("mardi")),
  (3l, ("mercredi"))
  )

val verticesRDD = sc.parallelize(vertices)
verticesRDD.take(1)

val edges = List(
  Edge(1L, 2L, 1800),
  Edge(2L, 3L,  800),
  Edge(3L, 1L, 1400)
)

val edgesRDD = sc.parallelize(edges)
edgesRDD.take(1)

val unknown = "unknown"
val graph = Graph(verticesRDD, edgesRDD, unknown)

graph.vertices.collect().foreach(println)
graph.edges.collect().foreach(println)

graph.numEdges