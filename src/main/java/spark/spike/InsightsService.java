package spark.spike;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.Tuple3;

import java.time.LocalDateTime;
import java.util.PriorityQueue;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class InsightsService {
    private static final int TOP_ELEMENTS_COUNT = 3;

    private JavaSparkContext javaSparkContext;
    private SparkConf sparkConf;

    public InsightsService(JavaSparkContext javaSparkContext, SparkConf sparkConf) {
        this.javaSparkContext = javaSparkContext;
        this.sparkConf = sparkConf;
    }

    public void getTop3DepartmentsAndStore() {

        System.out.println("Fetching data start" + LocalDateTime.now());
        CassandraJavaRDD<CassandraRow> rowJavaRDD = CassandraJavaUtil
                .javaFunctions(javaSparkContext)
                .cassandraTable("sales_data", "monthly_revenue");
        //Verify whether cassandra is connected or not
        System.out.println("Fetching data finished" + LocalDateTime.now());


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

        System.out.println("calculating top departments start" + LocalDateTime.now());

        JavaRDD<TopDepartments> topDepartments = getTop3Departments(rowJavaRDD, queue, seqFunc, combFunc);


        CassandraConnector connector = CassandraConnector.apply(sparkConf);
        System.out.println("calculating top departments finished" + LocalDateTime.now());

        try (Session session = connector.openSession()) {
            session.execute("CREATE TABLE IF NOT EXISTS sales_data.top_departments (customer_no text," +
                    " store_no TEXT, " +
                    "departments TEXT," +
                    " PRIMARY KEY (customer_no,store_no))");
        }

        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + LocalDateTime.now());
        CassandraJavaUtil.javaFunctions(topDepartments).writerBuilder("sales_data", "top_departments", mapToRow(TopDepartments.class)).saveToCassandra();
        System.out.println("Storing done >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + LocalDateTime.now());
    }

    private static JavaRDD<TopDepartments> getTop3Departments(CassandraJavaRDD<CassandraRow> rowJavaRDD, PriorityQueue<Tuple2<String, Double>> queue, Function2<PriorityQueue<Tuple2<String, Double>>, Tuple2<String, Double>, PriorityQueue<Tuple2<String, Double>>> seqFunc, Function2<PriorityQueue<Tuple2<String, Double>>, PriorityQueue<Tuple2<String, Double>>, PriorityQueue<Tuple2<String, Double>>> combFunc) {
        return rowJavaRDD.mapToPair(row -> {
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
                            return new TopDepartments(result._1()._1(), result._1()._2(), String.valueOf(top3Departments));
                        }
                );
    }
}
