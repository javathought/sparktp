package io.javathought.spk.tp;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class WordCount {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount");
		JavaSparkContext context = new JavaSparkContext(sparkConf);

		 String outputFile = "count-sherlock-2"; // args[0];
		 final List<String> stopWords = new ArrayList<String>();
		
		try {
			 getStopWords(stopWords);
		} catch (IOException | ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		
		JavaRDD<String> sherlockLines = context.textFile(WordCount.class.
                getClassLoader().getResource("sherlock.txt").getPath());
		
		sherlockLines.cache();
		
		JavaPairRDD<String, Integer> words = 
				sherlockLines
				.flatMap( l -> Arrays.asList(l.split("[ ’-”“'\",.]+")).iterator() )
				.filter( w -> ! stopWords.contains(w.toLowerCase()) )
				.mapToPair(w -> new Tuple2(w, 1));
		
		JavaPairRDD<String, Integer> counts = 
				words
				.reduceByKey(new Function2<Integer, Integer, Integer>(){
					public Integer call(Integer x, Integer y){ return x + y;}}
				);
		
		List<Tuple2<String, Integer>> top10 = counts.top(10, new WordComparator());
			
		top10.forEach(wc -> System.out.println(String.format("%s => %d", wc._1, wc._2))  );
		
		counts.saveAsTextFile(outputFile);
		
		context.close();

	}
	
	private static void getStopWords(List<String> stopWords) throws IOException, ParseException {
		JSONParser parser = new JSONParser();
		
        Object obj = parser.parse(new FileReader(WordCount.class.
                getClassLoader().getResource("stop-words-en.json").getFile()));

        JSONArray jsonObject = (JSONArray) obj;
		
		jsonObject.forEach(o -> stopWords.add(((String)o)));
		
	}
	
}
