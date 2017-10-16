package spark.spike;

import java.math.BigDecimal;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Serializable;
import scala.Tuple2;

class SalesService implements Serializable {

    Tuple2<BigDecimal, StoreBranch> findMedianSalesAtStoreAndBranchLevel() {
        SparkConfig sparkConfig = new SparkConfig();

        JavaPairRDD<CustomerIdentifier, String> branchMapping = new BranchImporter().importBranchMapping(sparkConfig.sqlContext);
        JavaPairRDD<CustomerIdentifier, BigDecimal> salesData = new SalesDataImporter().importFile(sparkConfig.javaSparkContext);

        JavaPairRDD<StoreBranch, BigDecimal> storeAndBranchToSalesMapping = branchMapping.join(salesData).mapToPair(this::mapToBranch);

        JavaPairRDD<StoreBranch, BigDecimal> storeAndBranchToAggregatedSalesMapping = storeAndBranchToSalesMapping
            .groupByKey()
            .mapToPair(this::aggregateAmountByBranch);

        return SparkUtil.findMedianFromRdd(storeAndBranchToAggregatedSalesMapping);
    }

    private Tuple2<StoreBranch, BigDecimal> aggregateAmountByBranch(
        Tuple2<StoreBranch, Iterable<BigDecimal>> tuple2) {
        BigDecimal totalAmount = new BigDecimal(0);

        for (BigDecimal amount : tuple2._2()) {
            totalAmount = totalAmount.add(amount);
        }
        return new Tuple2<>(tuple2._1(), totalAmount);
    }

    private Tuple2<StoreBranch, BigDecimal> mapToBranch(
        Tuple2<CustomerIdentifier, Tuple2<String, BigDecimal>> v1) {
        return new Tuple2<>(new StoreBranch(v1._1().getStoreNumber(), v1._2()._1()), v1._2()._2());
    }

}
