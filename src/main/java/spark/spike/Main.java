package spark.spike;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple3;
import spark.spike.model.result.SalesRow;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Main {

    private static final int TOP_ELEMENTS_COUNT = 3;
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        SparkConf sparkConf =
                new SparkConf()
                        .setAppName("Job - " + new Date())
                        .setMaster("spark://spark-master:7077");//	.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        SQLContext sqlContext = SparkSession
                .builder()
                .sparkContext(new SparkContext(sparkConf))
                .getOrCreate()
                .sqlContext();
    
        Properties connectionProperties = new Properties() {{
            setProperty("driver", "org.postgresql.Driver");
            setProperty("fetchSize", "1000");
        }};
        String jdbcUrl = "jdbc:postgresql://postgres:5432/testDB?user=test&password=test";
    
        Dataset<Row> rowDataset = sqlContext
                                          .read()
                                          .jdbc(jdbcUrl,
                                                  "monthly_revenue",
                                                  "id",
                                                  0, 10000L, 5,
                                                  connectionProperties
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
    
        JavaRDD<SalesRow> salesRowJavaRDD =
                rowJavaRDD
                        .mapToPair(row -> {
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
                .flatMap(result -> {
                    String customerNumber = result._1()._1();
                    String storeNumber = result._1()._2();
                
                    PriorityQueue<Tuple2<String, Double>> top3Departments = result._2();
                
                    return top3Departments
                                   .stream()
                                   .map(dept -> {
                                       SalesRow row = new SalesRow();
                                       row.setCustomer_number(customerNumber)
                                               .setStore_number(storeNumber)
                                               .setDepartment_number(dept._1())
                                               .setTotal_amount(dept._2());
                                       return row;
                                   })
                                   .iterator();
                });
        
        
        sqlContext.createDataFrame(Collections.emptyList(), SalesRow.class)
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(jdbcUrl, "result", connectionProperties);
        
        long count = salesRowJavaRDD.count();
        System.out.println("Count = " + count);
        
        
        salesRowJavaRDD
                .coalesce((int) (count / 1000) + 1)
                .foreachPartition(rows -> {
                    String query = "INSERT INTO result "
                                           + SalesRow.getColumnNames()
                                           + " VALUES ";
                    query += StreamSupport.stream(Spliterators.spliteratorUnknownSize(rows, 0), false)
                                     .map(SalesRow::toString)
                                     .collect(Collectors.joining(" , "));
                    
                    Class.forName("org.postgresql.Driver");
                    DriverManager
                            .getConnection(jdbcUrl)
                            .createStatement()
                            .executeUpdate(query);
                });
    }
}
