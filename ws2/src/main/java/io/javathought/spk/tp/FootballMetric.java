package io.javathought.spk.tp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.sum;


public class FootballMetric {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("footballMetrics");
		JavaSparkContext context = new JavaSparkContext(sparkConf);

        mostLicenseRdd(args[0], context);
//        mostLicenseSql(args[0], context);


		context.close();

	}

    private static void mostLicenseRdd(String arg, JavaSparkContext context) {
        JavaRDD<String> lines = context.textFile(arg);
        JavaRDD<String[]> rows = lines.map(l -> l.split(";"));
        JavaRDD<String[]> footballRows = rows.filter(r -> r[2].equals("111"));

        JavaPairRDD<String, Tuple2<Integer, Integer>> deptFootRows = footballRows
                .filter(r -> ! r[36].equals("."))
                .mapToPair( row -> {
                    String dept = row[0].substring(1, 3);
                    Integer pop = new Integer(row[36]);
                    Integer lic = new Integer(row[3]);
                    return new Tuple2<String, Tuple2<Integer, Integer>> (
                            dept,
                            new Tuple2<Integer, Integer>(pop, lic)
                    );
                })
                .reduceByKey((a, b) ->
                    new Tuple2<Integer, Integer>(a._1 + b._1, a._2 + b._2) );
        deptFootRows.mapValues( dept -> dept._2 * 1.0 / dept._1)
//                .collect()
                .top(10, new DeptComparator() )
                .forEach(r -> System.out.println(String.format("%s %f", r._1, r._2)));
    }

    private static void mostLicenseSql(String arg, JavaSparkContext context) {
        SQLContext sqlContext = new SQLContext(context);
        Dataset<Row> csv = sqlContext.read().format("csv").option("header", "true").load(arg);
        csv.cache();
        csv.select(sum(col("l_2012"))).where(col("fed_2012").equalTo(111)).show();
//		csv.select(substring(col("cog2"), 0, 2), col("l_2012"), col("pop_2010")).where(col("fed_2012").equalTo(111));

//        Dataset<Row> res.
    }


}
