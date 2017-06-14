import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import shapeless.ops.nat.LT.<
val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
val sc = new JavaSparkContext(sparkConf)

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.random.RandomRDDs

val data: RDD[Vector] = RandomRDDs.normalVectorRDD(sc, 1000L, 3)
println(data)