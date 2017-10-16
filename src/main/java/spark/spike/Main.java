package spark.spike;

import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

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
        JavaPairRDD<CustomerIdentifier, String> branchMapping = javaRDD
            .mapToPair((PairFunction<Row, CustomerIdentifier, String>) row -> new Tuple2<>(new CustomerIdentifier(row.getAs("store_number"), row.getAs("customer_number")),
                row.getAs("metro_branch")));

        JavaRDD<String> salesData = javaSparkContext
            .textFile("/src/main/resources/MDW_hcmrevenue_v001_20160531_20160602_024440.csv", 0);

        String first = salesData.first();
        JavaPairRDD<CustomerIdentifier, BigDecimal> salesMapping = salesData.filter(row -> !row.equals(first))
            .mapToPair(Main::mapToSalesData);

        System.out.println("!!!!!!!!!!!!!!!!!!!!!! Sales Mapping count -" + salesMapping.count());
        List<Tuple2<CustomerIdentifier, BigDecimal>> take = salesMapping.take(1);

        System.out.println("!!!!!!!!!!!!!!!!!!!!!! Sales Mapping first key -" + take.get(0)._1());
        System.out.println("!!!!!!!!!!!!!!!!!!!!!! Sales Mapping first value     -" + take.get(0)._2());

        JavaPairRDD<CustomerIdentifier, Tuple2<String, BigDecimal>> branchToSales = branchMapping.join(salesMapping);

        System.out.println("------------------- branch to sales count " + branchToSales);

        JavaPairRDD<CustomerBranch, BigDecimal> storeToCustomerToBranch = branchToSales.mapToPair(Main::mapToBranch);
        JavaPairRDD<CustomerBranch, Iterable<BigDecimal>> rdd = storeToCustomerToBranch.groupByKey();

        System.out.println("Printing rdd --------------------- ");
        rdd.take(10).forEach(mapping -> System.out.println(mapping._1().getBranch() + " ///"
            + mapping._1().getCustomerIdentifier().getCustomerNumber() + " ///"
            + mapping._1().getCustomerIdentifier().getStoreNumber() + " ///"
            + mapping._2().toString()));

        JavaPairRDD<CustomerBranch, BigDecimal> mappedToStoreAmounts = rdd.mapToPair(Main::aggregateAmountByBranch);

        mappedToStoreAmounts.take(10).forEach(mapping -> System.out.println(mapping._1().getBranch() + " ///"
            + mapping._1().getCustomerIdentifier().getCustomerNumber() + " ///"
            + mapping._1().getCustomerIdentifier().getStoreNumber() + " ///"
            + mapping._2()));
    }

    private static Tuple2<CustomerBranch, BigDecimal> aggregateAmountByBranch(
        Tuple2<CustomerBranch, Iterable<BigDecimal>> tuple2) {
        BigDecimal totalAmount = new BigDecimal(0);

        for (BigDecimal amount : tuple2._2()) {
            totalAmount = totalAmount.add(amount);
        }
        return new Tuple2<>(tuple2._1(), totalAmount);
    }

    private static Tuple2<CustomerIdentifier, BigDecimal> mapToSalesData(String v1) {
        String[] split = v1.split("\t");

        SalesData salesData = new SalesData().builder()
            .customer_Number(removeLeadingZero(removeQuotes(split[1])))
            .store_Number(removeLeadingZero(removeQuotes(split[2])))
            .mtdAmount(BigDecimal.valueOf(Float.parseFloat(removeQuotes(split[13]))))
            .build();
        return new Tuple2<>(new CustomerIdentifier(salesData.getStore_Number(), salesData.getCustomer_Number()), salesData.getMtdAmount());

    }

    private static String removeLeadingZero(String value) {
        return value.replaceFirst("^0+", "");
    }

    private static String removeQuotes(String s) {
        return s.replaceAll("\\\"", "");
    }

    private static Tuple2<CustomerBranch, BigDecimal> mapToBranch(
        Tuple2<CustomerIdentifier, Tuple2<String, BigDecimal>> v1) {
        return new Tuple2<>(new CustomerBranch(v1._1(), v1._2()._1()), v1._2()._2());
    }
}
