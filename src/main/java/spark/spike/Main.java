package spark.spike;

import java.math.BigDecimal;
import scala.Tuple2;

public class Main {
    public static void main(String[] args) {
        SalesService service = new SalesService();
        Tuple2<BigDecimal, StoreBranch> tuple2 = service.findMedianSalesAtStoreAndBranchLevel();

        System.out.println(tuple2._1() + " ------------------------------ " + tuple2._2().getStoreNumber() + "++++++++++++++++" + tuple2._2.getBranch());
    }
}
