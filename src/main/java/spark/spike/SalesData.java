package spark.spike;

import com.sun.xml.internal.ws.developer.Serialization;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@Builder
@Serialization
@ToString
@AllArgsConstructor
public class SalesData implements Serializable {
    private String customer_Number;
    private String store_Number;
    private Date mtd_Date;
    private Integer department;
    private Integer invoice_type;
    private BigDecimal mtdAmount;
}
