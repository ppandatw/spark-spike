package spark.spike;

import java.util.Date;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

class SparkConfig {
    private SparkConf sparkConf = new SparkConf().setAppName("Job - " + new Date());

    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
    private SparkContext context = javaSparkContext.sc();

    SQLContext sqlContext = SparkSession
        .builder()
        .sparkContext(context)
        .getOrCreate()
        .sqlContext();
}
