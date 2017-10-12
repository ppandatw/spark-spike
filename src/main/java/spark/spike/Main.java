package spark.spike;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Date;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.stream.Collectors;

public class Main {

    private static final int TOP_ELEMENTS_COUNT = 3;

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
                        "monthly_revenue_large",
                        "id",
                        0, 83865972L, 500,
                        new Properties() {{
                            setProperty("driver", "org.postgresql.Driver");
                            setProperty("fetchSize", "1000");
                        }}
                );

        JavaRDD<Row> rowJavaRDD = rowDataset.javaRDD();

//        PriorityQueue<Tuple2<String, Double>> queue = new PriorityQueue<>(TOP_ELEMENTS_COUNT, Comparator.comparing(Tuple2::_2));
        PriorityQueue<Tuple2<String, Double>> queue = new PriorityQueue<>(TOP_ELEMENTS_COUNT, DepartmentComparator.instance);

        Function2<PriorityQueue<Tuple2<String, Double>>, Tuple2<String, Double>, PriorityQueue<Tuple2<String, Double>>> seqFunc = (q, v) -> {
            q.add(v);
            while (q.size() > TOP_ELEMENTS_COUNT) {
                q.poll();
            }
            return q;
        };

        Function2<PriorityQueue<Tuple2<String, Double>>, PriorityQueue<Tuple2<String, Double>>, PriorityQueue<Tuple2<String, Double>>> combFunc = (q1, q2) -> {
            q1.addAll(q2);
            while (q1.size() > TOP_ELEMENTS_COUNT) {
                q1.poll();
            }
            return q1;
        };

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
                .aggregateByKey(queue,
                        seqFunc,
                        combFunc
                )
                .map(result -> {
                            PriorityQueue<Tuple2<String, Double>> top3Departments = result._2();
//                            String stringRepresentationOfTop3Dept = top3Departments.stream()
//                                    .map(dept -> " dept : " + dept._1() + " , "
//                                            + "amount : " + dept._2()
//                                    )
//                                    .collect(Collectors.joining(","));

                            return "customer_no = " + result._1()._1() + ", "
                                    + "store_no = " + result._1()._2()
                                    + " : " + top3Departments;
                        }
                )
                .collect()
                .forEach(System.out::println);
    }
}
