package spark.spike;

import java.math.BigDecimal;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

class SparkUtil implements Serializable {
    static Tuple2<BigDecimal, StoreBranch> findMedianFromRdd(
        JavaPairRDD<StoreBranch, BigDecimal> storeAndBranchToAggregatedSalesMapping) {
        JavaPairRDD<BigDecimal, StoreBranch> reversedBranchToAmount = storeAndBranchToAggregatedSalesMapping
            .mapToPair((PairFunction<Tuple2<StoreBranch, BigDecimal>, BigDecimal, StoreBranch>) tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1()))
            .sortByKey();

        long count = reversedBranchToAmount.count();

        JavaPairRDD<Long, Tuple2<BigDecimal, StoreBranch>> rdd = reversedBranchToAmount.zipWithIndex()
            .mapToPair(tuple2 -> new Tuple2<>(tuple2._2(), tuple2._1()));

        return rdd.lookup(count / 2).get(0);
    }
}
