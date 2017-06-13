package io.javathought.spk.tp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

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


public class Anagramme {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount");
		JavaSparkContext context = new JavaSparkContext(sparkConf);

		JavaRDD<String> frenchWords = context.textFile(args[0]);
		 String outputFile = args[1]; 
		
		frenchWords.cache();
		
		JavaPairRDD<String, String> words = 
				frenchWords
				.mapToPair(w -> new Tuple2(sort(w), w));
		
		JavaRDD<List<String>> anagrammes = 
				words
//				.reduceByKey(new Function2<String, String, String>(){
//					public String call(String x, String y){ return x + " " + y;}}
//				)
				.aggregateByKey( 
					new ArrayList<String>(), 
					new Function2<List<String>, String, List<String>>(){
						public List<String> call(List<String> a, String x){ 
							a.add(x);
							return a;}
						},
					new Function2<List<String>, List<String>, List<String>>(){
							public List<String> call(List<String> a, List<String> b){ 
								a.addAll(b);
								return a;}
						}
					
				)
				.filter(lw -> lw._2.size() > 1)
				.map(a -> a._2)
				;
						
		anagrammes.saveAsTextFile(outputFile);
		
		context.close();

	}
	
	private static String sort(String original)  {
	    Collator collator = Collator.getInstance(new Locale("fr", "FR"));
	    String[] split = original.split("");
	    Arrays.sort(split, collator);
	    String sorted = "";
	    for (int i = 0; i < split.length; i++)
	    {
	      sorted += split[i];
	    }
	    
	    return sorted;
	}
	
}
