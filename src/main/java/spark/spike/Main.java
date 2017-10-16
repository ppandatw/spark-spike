package spark.spike;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Date;

public class Main {

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


        InsightsService insightsService = new InsightsService(javaSparkContext, sparkConf);
        CustomerService customerService = new CustomerService(javaSparkContext, sparkConf);
        //TODO: to get top 3 departments
        insightsService.getTop3DepartmentsAndStore();

        //TODO: to get sales data @store and @branch level
        customerService.getSalesAtBranchAndStoreLevel();
    }
}
