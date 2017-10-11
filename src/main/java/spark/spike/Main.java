package spark.spike;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Main {
  public static void main(String[] args) throws ClassNotFoundException {
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("Job - " + new Date())
            .setMaster("spark://spark-master:7077");//	.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    SQLContext sqlContext = SparkSession
        .builder()
        .sparkContext(new SparkContext(sparkConf))
        .getOrCreate()
        .sqlContext();

    Dataset<Row> rowDataset = sqlContext
        .read()
            .jdbc("jdbc:postgresql://postgres:5432/testDB?user=test&password=test",
                    "monthly_revenue",
                    "id",
                    0, 11764704, 100,
                    new Properties(){{
                        setProperty("driver", "org.postgresql.Driver");
                        setProperty("fetchSize", "1000");
                    }}
            );

    JavaRDD<Row> rowJavaRDD = rowDataset.javaRDD();

    rowJavaRDD.mapToPair(row -> {
      Tuple3<String, String, String> key = new Tuple3<>(row.getString(0), row.getString(1), row.getString(3));
      Double value = row.getDouble(5);
      return new Tuple2<>(key, value);
    })
        .reduceByKey(Double::sum)
        .mapToPair(tuple -> {
          Tuple3<String, String, String> oldKey = tuple._1();
          Double sumAmount = tuple._2();

          Tuple2<String, String> newKey = new Tuple2<>(oldKey._1(), oldKey._2());
          Tuple2<String, Double> newValue = new Tuple2<>(oldKey._3(), sumAmount);
          return new Tuple2<>(newKey, newValue);
        })
        .reduceByKey((x, y) -> x._2() > y._2() ? x : y)
        .map(result -> "customer_no = " + result._1()._1() + ", "
            + "store_no = " + result._1()._2() + ", "
            + "department = " + result._2()._1() + ", "
            + "amount = " + result._2()._2()
        )
        .collect()
        .forEach(System.out::println);
  }
}
