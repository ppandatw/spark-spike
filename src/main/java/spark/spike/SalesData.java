package spark.spike;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@Builder
@ToString
@Getter
@AllArgsConstructor
class SalesData implements Serializable {
    private String customer_Number;
    private String store_Number;
    private Date mtd_Date;
    private Integer department;
    private Integer invoice_type;
    private BigDecimal mtdAmount;
}
