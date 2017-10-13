package spark.spike;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Date;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import static java.time.LocalDate.parse;
import static java.time.ZoneId.systemDefault;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

public class Main {

    private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(YEAR, 4)
        .appendValue(MONTH_OF_YEAR, 2)
        .appendValue(DAY_OF_MONTH, 2)
        .toFormatter();

    public static void main(String[] args) {
        SparkConf sparkConf =
            new SparkConf().setAppName("Job - " + new Date());

//        SparkContext context = new SparkContext(sparkConf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SparkContext context = javaSparkContext.sc();
        SQLContext sqlContext = SparkSession
            .builder()
            .sparkContext(context)
            .getOrCreate()
            .sqlContext();

        Dataset<Row> rowDataset = sqlContext
            .read()
            .option("fetchSize", "1000")
            .jdbc("jdbc:postgresql://postgres:5432/testDB?user=test&password=test",
                "portfolio",
                new Properties() {{
                    setProperty("driver", "org.postgresql.Driver");
                }}
            );

        JavaRDD<Row> javaRDD = rowDataset
            .javaRDD();
        System.out.println("******************************" + javaRDD.take(10).size());
        JavaPairRDD<String, String> branchMapping = javaRDD
            .mapToPair((PairFunction<Row, String, String>) row -> new Tuple2<>(row.getAs("store_number") + "_" + row.getAs("customer_number"),
                row.getAs("metro_branch")));

        JavaRDD<String> salesData = javaSparkContext
            .textFile("/src/main/resources/MDW_hcmrevenue_v001_20160531_20160602_024440.csv", 0);

        String first = salesData.first();
        JavaPairRDD<String, BigDecimal> salesMapping = salesData.filter(row -> !row.equals(first))
            .mapToPair(Main::func);

        JavaPairRDD<String, Tuple2<String, BigDecimal>> branchToSales = branchMapping.join(salesMapping);
        System.out.println("branch to sales count " + branchToSales.count());

        JavaPairRDD<String, Tuple2<String, BigDecimal>> rdd = branchToSales.reduceByKey((Function2<Tuple2<String, BigDecimal>, Tuple2<String, BigDecimal>,
            Tuple2<String, BigDecimal>>) (v1, v2) ->
            new Tuple2<>(v1._1, v1._2().add(v2._2())));
        System.out.println("Count " + rdd.count());
    }

    private static Tuple2<String, BigDecimal> func(String v1) {
        String[] split = v1.split("\t");

        SalesData salesData = new SalesData().builder()
            .customer_Number(removeLeadingZero(split[1]))
            .store_Number(removeLeadingZero(split[2]))
//            .mtd_Date(toFirstDayOfMonth(removeQuotes(split[4])))
//            .department(Integer.parseInt(removeQuotes(split[7])))
//            .invoice_type(Integer.parseInt(removeQuotes(split[8])))
            .mtdAmount(BigDecimal.valueOf(Long.parseLong(removeQuotes(split[8]))))
            .build();
        return new Tuple2<>(salesData.getStore_Number() + "_" + salesData.getCustomer_Number(), salesData.getMtdAmount());

    }

    private static String removeLeadingZero(String value) {
        return value.replaceFirst("^0+", "");
    }

    private static String removeQuotes(String s) {
        return s.replaceAll("\\\"", "");
    }

    private static ZonedDateTime toZonedDateTime(String dateAsString) {
        return parse(dateAsString, DATE_FORMATTER)
            .atStartOfDay(systemDefault());
    }

}
