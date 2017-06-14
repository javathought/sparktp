package io.javathought.spk.tp;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import redis.clients.jedis.Jedis;

import java.util.Arrays;


/**
 *
 */
public class StreamingCheck {

    public static void main(String[] args) {
        System.out.print("Flume streaming test");

        SparkConf conf = new SparkConf().setAppName("Spark Streaming Test").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));

        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
                FlumeUtils.createStream(ssc, "localhost", 4444);
//                FlumeUtils.createPollingStream(ssc, "localhost", 4444);
        // pollingStream generate Connection refused exception

        Jedis jedis = new Jedis("localhost");


        JavaDStream<String> datas = flumeStream.map(e -> new String(e.event().getBody().array(), java.nio.charset.StandardCharsets.UTF_8));

        JavaDStream<String> words = datas
                .flatMap(w -> Arrays.asList(w.split(" ")).iterator());


        words.cache();
        words.print();
        words.count().print();

                ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {}


    }
}
