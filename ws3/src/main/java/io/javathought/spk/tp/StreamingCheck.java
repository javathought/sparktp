package io.javathought.spk.tp;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import redis.clients.jedis.Jedis;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;


/**
 *
 */
public class StreamingCheck {

    public static final String LOCALHOST = "localhost";

    public static void main(String[] args) {
        System.out.print("Flume streaming test");

        SparkConf conf = new SparkConf().setAppName("Spark Streaming Test").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));

        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
                FlumeUtils.createStream(ssc, "localhost", 4444);
//                FlumeUtils.createPollingStream(ssc, "localhost", 4444);
        // pollingStream generate Connection refused exception

        final Jedis jedis = new Jedis(LOCALHOST);

        JavaDStream<String> datas = flumeStream.map(e -> new String(e.event().getBody().array(), java.nio.charset.StandardCharsets.UTF_8));

        JavaDStream<String> words = datas
                .flatMap(w -> Arrays.asList(w.split(" ")).iterator())
                .map(String::toLowerCase);

        words.cache();

        JavaPairDStream<String, Integer> wc = words
                .mapToPair(s -> new Tuple2<String, Integer>(s, 1))
                .reduceByKey((b, a) -> a + b);


        words.print();
        words.count().print();

        jedis.incrBy("runned tasks", 1);

        wc.foreachRDD(rdd -> rdd.collect().forEach(p -> jedis.incrBy(p._1, p._2)));

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {}
        jedis.close();


    }

    private class Incr implements Serializable {
        public void apply(Jedis jedis, Tuple2<String, Integer> p)  {
            jedis.incrBy(p._1, p._2);
        }
    }

}
