package spark.spike;

import java.math.BigDecimal;
import java.util.Properties;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

public class Main {

    public static void main(String[] args) {
        SparkConfig sparkConfig = new SparkConfig();
        SQLContext sqlContext = sparkConfig.getSqlContext();
        JavaSparkContext context = sparkConfig.getJavaSparkContext();

        JavaRDD<Row> javaRDD = sqlContext
            .read()
            .option("fetchSize", "1000")
            .jdbc("jdbc:postgresql://postgres:5432/testDB?user=test&password=test",
                "portfolio",
                new Properties() {{
                    setProperty("driver", "org.postgresql.Driver");
                }}
            ).javaRDD();

        JavaPairRDD<CustomerIdentifier, String> branchMapping = javaRDD
            .mapToPair((PairFunction<Row, CustomerIdentifier, String>) row -> new Tuple2<>(new CustomerIdentifier(row.getAs("store_number"), row.getAs("customer_number")),
                row.getAs("metro_branch")));

        JavaRDD<String> salesData = context
            .textFile("/src/main/resources/MDW_hcmrevenue_v001_20160531_20160602_024440.csv", 0);

        JavaPairRDD<CustomerIdentifier, BigDecimal> salesDataWithoutHeaders = eliminateHeadersFromTextFile(salesData);

        JavaPairRDD<CustomerIdentifier, Tuple2<String, BigDecimal>> branchToSalesMapping = branchMapping.join(salesDataWithoutHeaders);
        JavaPairRDD<StoreBranch, BigDecimal> storeToBranchMapping = branchToSalesMapping.mapToPair(Main::mapToBranch);

        JavaPairRDD<StoreBranch, BigDecimal> aggregatedAmountByBranchMapping = storeToBranchMapping
            .groupByKey()
            .mapToPair(Main::aggregateAmountByBranch);

        JavaPairRDD<BigDecimal, StoreBranch> reversedBranchToAmount = aggregatedAmountByBranchMapping
            .mapToPair((PairFunction<Tuple2<StoreBranch, BigDecimal>, BigDecimal, StoreBranch>) tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1()))
            .sortByKey();

        long count = reversedBranchToAmount.count();

        JavaPairRDD<Long, Tuple2<BigDecimal, StoreBranch>> rdd = reversedBranchToAmount.zipWithIndex()
            .mapToPair(tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1()));

        Tuple2<BigDecimal, StoreBranch> tuple2 = rdd.lookup(count / 2).get(0);
        System.out.println(tuple2._1() + " ------------------------------ " + tuple2._2().getStoreNumber() + "++++++++++++++++" + tuple2._2.getBranch());
    }

    private static void printBranchAndCustomerToAggregatedAmounts(
        JavaPairRDD<BigDecimal, StoreBranch> mappedToStoreAmounts) {
        mappedToStoreAmounts.take(10).forEach(mapping -> System.out.println(mapping._2().getBranch() + " ///"
            + mapping._2().getStoreNumber() + " ///"
            + mapping._1()));
    }

    private static JavaPairRDD<CustomerIdentifier, BigDecimal> eliminateHeadersFromTextFile(JavaRDD<String> salesData) {
        String first = salesData.first();
        return salesData.filter(row -> !row.equals(first))
            .mapToPair(Main::mapToSalesData);
    }

    private static Tuple2<StoreBranch, BigDecimal> aggregateAmountByBranch(
        Tuple2<StoreBranch, Iterable<BigDecimal>> tuple2) {
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

    private static Tuple2<StoreBranch, BigDecimal> mapToBranch(
        Tuple2<CustomerIdentifier, Tuple2<String, BigDecimal>> v1) {
        return new Tuple2<>(new StoreBranch(v1._1().getStoreNumber(), v1._2()._1()), v1._2()._2());
    }
}
