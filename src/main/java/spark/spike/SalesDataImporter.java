package spark.spike;

import java.math.BigDecimal;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

class SalesDataImporter implements Serializable{
    JavaPairRDD<CustomerIdentifier, BigDecimal> importFile(JavaSparkContext javaSparkContext) {
        JavaRDD<String> salesData = javaSparkContext
            .textFile("/src/main/resources/MDW_hcmrevenue_v001_20160531_20160602_024440.csv", 0);

        return eliminateHeadersFromTextFile(salesData);
    }

    private JavaPairRDD<CustomerIdentifier, BigDecimal> eliminateHeadersFromTextFile(JavaRDD<String> salesData) {
        String first = salesData.first();
        return salesData.filter(row -> !row.equals(first))
            .mapToPair(this::mapToSalesData);
    }

    private Tuple2<CustomerIdentifier, BigDecimal> mapToSalesData(String v1) {
        String[] split = v1.split("\t");

        SalesData salesData = SalesData.builder()
            .customer_Number(removeLeadingZero(removeQuotes(split[1])))
            .store_Number(removeLeadingZero(removeQuotes(split[2])))
            .mtdAmount(BigDecimal.valueOf(Float.parseFloat(removeQuotes(split[13]))))
            .build();
        return new Tuple2<>(new CustomerIdentifier(salesData.getStore_Number(), salesData.getCustomer_Number()), salesData.getMtdAmount());
    }

    private String removeLeadingZero(String value) {
        return value.replaceFirst("^0+", "");
    }

    private String removeQuotes(String s) {
        return s.replaceAll("\"", "");
    }

}
