package spark.spike;

import java.math.BigDecimal;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class Main {

    public static void main(String[] args) {
        SparkConfig sparkConfig = new SparkConfig();

        JavaPairRDD<CustomerIdentifier, String> branchMapping = new BranchImporter().importBranchMapping(sparkConfig.sqlContext);
        JavaPairRDD<CustomerIdentifier, BigDecimal> salesData = new SalesDataImporter().importFile(sparkConfig.javaSparkContext);

        JavaPairRDD<StoreBranch, BigDecimal> storeAndBranchToSalesMapping = branchMapping.join(salesData).mapToPair(Main::mapToBranch);

        JavaPairRDD<StoreBranch, BigDecimal> storeAndBranchToAggregatedSalesMapping = storeAndBranchToSalesMapping
            .groupByKey()
            .mapToPair(Main::aggregateAmountByBranch);

        Tuple2<BigDecimal, StoreBranch> tuple2 = SparkUtil.findMedianFromRdd(storeAndBranchToAggregatedSalesMapping);

        System.out.println(tuple2._1() + " ------------------------------ " + tuple2._2().getStoreNumber() + "++++++++++++++++" + tuple2._2.getBranch());
    }

    private static Tuple2<StoreBranch, BigDecimal> aggregateAmountByBranch(
        Tuple2<StoreBranch, Iterable<BigDecimal>> tuple2) {
        BigDecimal totalAmount = new BigDecimal(0);

        for (BigDecimal amount : tuple2._2()) {
            totalAmount = totalAmount.add(amount);
        }
        return new Tuple2<>(tuple2._1(), totalAmount);
    }

    private static Tuple2<StoreBranch, BigDecimal> mapToBranch(
        Tuple2<CustomerIdentifier, Tuple2<String, BigDecimal>> v1) {
        return new Tuple2<>(new StoreBranch(v1._1().getStoreNumber(), v1._2()._1()), v1._2()._2());
    }
}
