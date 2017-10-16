package spark.spike;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.math.BigDecimal;

@AllArgsConstructor
@Getter
public class BranchLevelSalesData {
    private String customerNumber;
    private String storeNumber;
    private String branch;
    private BigDecimal amount;
}
