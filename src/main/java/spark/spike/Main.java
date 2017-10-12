package spark.spike;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Date;

public class Main {
    private static final int TOP_ELEMENTS_COUNT = 3;

    public static void main(String[] args) throws ClassNotFoundException {
        SparkConf sparkConf =
                new SparkConf()
                        .setAppName("Job - " + new Date())
                        .setMaster("spark://spark-master:7077");//	.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        sparkConf.set("spark.cassandra.connection.host", "cassandra")
//                .set("spark.cassandra.connection.native.port", "9042")
//                .set("spark.cassandra.connection.rpc.port", "9160")
//                .set("spark.cassandra.connection.timeout_ms", "5000")
//                .set("spark.cassandra.read.timeout_ms", "200000")
                .set("spark.cassandra.auth.username", "cassandra")
                .set("spark.cassandra.auth.password", "cassandra");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        CassandraJavaRDD<CassandraRow> rowJavaRDD = CassandraJavaUtil
                .javaFunctions(javaSparkContext)
                .cassandraTable("insights", "monthly_revenue");

        //Verify whether cassandra is connected or not
        rowJavaRDD
                .take(10)
                .forEach(System.out::println);


//TODO : following code works with postgres

////        PriorityQueue<Tuple2<String, Double>> queue = new PriorityQueue<>(TOP_ELEMENTS_COUNT, Comparator.comparing(Tuple2::_2));
//        PriorityQueue<Tuple2<String, Double>> queue = new PriorityQueue<>(TOP_ELEMENTS_COUNT, DepartmentComparator.instance);
//
//        Function2<PriorityQueue<Tuple2<String, Double>>, Tuple2<String, Double>, PriorityQueue<Tuple2<String, Double>>> seqFunc = (q, v) -> {
//            q.add(v);
//            while (q.size() > TOP_ELEMENTS_COUNT) {
//                q.poll();
//            }
//            return q;
//        };
//
//        Function2<PriorityQueue<Tuple2<String, Double>>, PriorityQueue<Tuple2<String, Double>>, PriorityQueue<Tuple2<String, Double>>> combFunc = (q1, q2) -> {
//            q1.addAll(q2);
//            while (q1.size() > TOP_ELEMENTS_COUNT) {
//                q1.poll();
//            }
//            return q1;
//        };
//
//
//        rowJavaRDD.mapToPair(row -> {
//            Tuple3<String, String, String> key = new Tuple3<>(row.getString(0), row.getString(1), row.getString(3));
//            Double value = row.getDouble(5);
//            return new Tuple2<>(key, value);
//        })
//                .reduceByKey(Double::sum)
//                .mapToPair(tuple -> {
//                    Tuple3<String, String, String> oldKey = tuple._1();
//                    Double sumAmount = tuple._2();
//
//                    Tuple2<String, String> newKey = new Tuple2<>(oldKey._1(), oldKey._2());
//                    Tuple2<String, Double> newValue = new Tuple2<>(oldKey._3(), sumAmount);
//                    return new Tuple2<>(newKey, newValue);
//                })
//                .aggregateByKey(queue,
//                        seqFunc,
//                        combFunc
//                )
//                .map(result -> {
//                            PriorityQueue<Tuple2<String, Double>> top3Departments = result._2();
////                            String stringRepresentationOfTop3Dept = top3Departments.stream()
////                                    .map(dept -> " dept : " + dept._1() + " , "
////                                            + "amount : " + dept._2()
////                                    )
////                                    .collect(Collectors.joining(","));
//
//                            return "customer_no = " + result._1()._1() + ", "
//                                    + "store_no = " + result._1()._2()
//                                    + " : " + top3Departments;
//                        }
//                )
//                .collect()
//                .forEach(System.out::println);
    }
}
