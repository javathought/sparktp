package io.javathought.spk.tp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.*;


public class FootballMetric {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("footballMetrics");
		JavaSparkContext context = new JavaSparkContext(sparkConf);

//		nettoie(args[0]);

//        mostLicenseRdd(args[0], context);
        mostLicenseSql(args[0], args[1], context);

		context.close();

	}

    private static void nettoie(String arg) {

            Charset charset = Charset.forName("UTF-8");
            Path path = Paths.get(arg);
            Path pathOut = Paths.get(arg + "new");
            try (BufferedReader reader = Files.newBufferedReader(path, charset);
                    BufferedWriter writer = Files.newBufferedWriter(pathOut, charset)) {
                String line = null;
                while ((line = reader.readLine()) != null) {

                    writer.write(line.substring(1, line.length() - 1) + "\n");
                }
            } catch (IOException x) {
                System.err.format("IOException: %s%n", x);
            }
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
        JavaPairRDD<String, Double> stats = deptFootRows.mapValues( dept -> dept._2 * 1.0 / dept._1);
        stats.cache();
//                .collect()
        stats.top(10, new DeptComparator() )
                .forEach(r -> System.out.println(String.format("%s %f", r._1, r._2)));

        System.out.println("-------------------------------------------------");
        stats.lookup("75").forEach(System.out::println);

    }

    private static void mostLicenseSql(String datas, String fedesFiles, JavaSparkContext context) {
        SQLContext sqlContext = new SQLContext(context);
        Dataset<Row> csv = sqlContext.read()
                .format("csv")
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .load(datas);
        csv.cache();
		Dataset<Row> resultat = csv.select(col("fed_2012"), col("l_2012"))
                .groupBy(col("fed_2012")).sum("l_2012");

        resultat
                .orderBy(col("sum(l_2012)").desc())
                .limit(10)
                .show();

/*        Dataset<Row> myRows = sqlContext.read()
                .format("jdbc")
                .option("url","jdbc:mysql://localhost")
                .option("dbtable", "spk.stat_licences")
                .option("user", "root")
                .option("password", "admin")
                .load();

        myRows.printSchema();*/

        Dataset<Row> fedes = sqlContext.read()
                .format("json")
                .load(fedesFiles);

        fedes.show();

        resultat
                .withColumnRenamed("fed_2012", "code")
                .withColumnRenamed("sum(l_2012)", "nb")
                .join(fedes, "code")
                .write().mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("url","jdbc:mysql://localhost")
                .option("dbtable", "spk.stat_licences")
                .option("user", "root")
                .option("password", "admin")
        .save();

    }




}
