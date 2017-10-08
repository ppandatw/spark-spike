package spark.spike;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;

public class Main {
	public static void main(String[] args) throws ClassNotFoundException {
		SparkConf sparkConf =
				new SparkConf()
						.setAppName("Square Batch job")
						.setMaster("spark://spark-master:7077");//	.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		SQLContext sqlContext = new SQLContext(new SparkContext(sparkConf));
		Map<String, String> options = new HashMap<>();
		options.put("url", "jdbc:postgresql://postgres:5432/testDB?user=test&password=test");
		options.put("dbtable", "ord_rev");
		options.put("driver", "org.postgresql.Driver");
		
		Dataset<Row> rowDataset = sqlContext.load("jdbc", options);
		
		JavaRDD<Row> rowJavaRDD = rowDataset.javaRDD();
		long count = rowJavaRDD.count();
		System.out.println("Number of rows in the view : " + count);
		
		rowJavaRDD.mapToPair(row -> {
			String customer = "customer - " + row.getString(0) + "::" + row.getString(1);
			return new Tuple2<String, Double>(customer, row.getDouble(3));
		})
				.groupByKey()
				.map((Tuple2<String, Iterable<Double>> tuple) -> new Tuple2(tuple._1(),
																				   StreamSupport.stream(tuple._2().spliterator(), false)
																						   .max(Double::compare)
																						   .orElse(-1.1)
				))
//				.take(10)
//				.forEach(System.out::println);
				.collect()
				.forEach(System.out::println);
		
		
//		new SparkBatchJobSquare(sparkConf, 1000)
//				.run();
	}
}
