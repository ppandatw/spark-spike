package spark.spike;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;

@Getter
public class BranchAndStoreLevelSales extends Tuple2<BranchAndStoreLevelSales.CustomerIdentifier, BigDecimal> implements Serializable {
    private CustomerIdentifier customerIdentifier;
    private BigDecimal amount;

    public BranchAndStoreLevelSales(CustomerIdentifier customerIdentifier, BigDecimal amount) {
        super(customerIdentifier, amount);
        this.amount = amount;
    }

    public BranchAndStoreLevelSales add(BranchAndStoreLevelSales other) {
        return new BranchAndStoreLevelSales(
                this.customerIdentifier,
                this.amount.add(other.amount)
        );
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    static class CustomerIdentifier implements Serializable {
        private String customerNumber;
        private String storeNumber;
        private String branch;
    }
}
