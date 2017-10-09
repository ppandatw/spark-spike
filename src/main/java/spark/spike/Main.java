package spark.spike;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class Main {
  public static void main(String[] args) throws ClassNotFoundException {
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("Square Batch job")
            .setMaster("spark://spark-master:7077");//	.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    SQLContext sqlContext = SparkSession
        .builder()
        .sparkContext(new SparkContext(sparkConf))
        .getOrCreate()
        .sqlContext();
    Map<String, String> options = new HashMap<>();
    options.put("url", "jdbc:postgresql://postgres:5432/testDB?user=test&password=test");
    options.put("driver", "org.postgresql.Driver");
    options.put("dbtable", "ord_rev");

    Dataset<Row> rowDataset = sqlContext
        .read()
        .format("jdbc")
        .options(options)
        .load();

    JavaRDD<Row> rowJavaRDD = rowDataset.javaRDD();
    long count = rowJavaRDD.count();
    System.out.println("Number of rows in the view : " + count);

    rowJavaRDD.mapToPair(row -> {
      String customer = "customer - " + row.getString(0) + "::" + row.getString(1);
      return new Tuple2<>(customer, row.getDouble(3));
    })
        .reduceByKey(Math::max)
        .collect()
        .forEach(System.out::println);
  }
}
