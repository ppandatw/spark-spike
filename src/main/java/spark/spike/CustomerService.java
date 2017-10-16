package spark.spike;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class CustomerService {

    private JavaSparkContext javaSparkContext;
    private SparkConf sparkConf;

    public CustomerService(JavaSparkContext javaSparkContext, SparkConf sparkConf) {
        this.javaSparkContext = javaSparkContext;
        this.sparkConf = sparkConf;
    }


    public void getSalesAtBranchAndStoreLevel() {
        CassandraJavaRDD<CassandraRow> customers = readData("portfolio", "de_portfolio");
        CassandraJavaRDD<CassandraRow> salesData = readData("sales_data", "monthly_revenue");

        JavaPairRDD<Tuple2<String, String>, String> branchMapping = customers
                .mapToPair(row ->
                        new Tuple2<>(
                                new Tuple2<>(row.getString("store_number"), row.getString("customer_number")),
                                row.getString("metro_branch")));

        JavaPairRDD<Tuple2<String, String>, BigDecimal> salesMapping = salesData.mapToPair(row ->
                new Tuple2<>(
                        new Tuple2<>(row.getString("store_number"), row.getString("customer_number")),
                        new BigDecimal(row.getString("amount"))));

        JavaPairRDD<Tuple2<String, String>, Tuple2<String, BigDecimal>> branchToSalesMapping = branchMapping.join(salesMapping);

        JavaPairRDD<BranchAndStoreLevelSales.CustomerIdentifier, BigDecimal> customerIdentifierVsAmountRDD = branchToSalesMapping.mapToPair(CustomerService::mapToBranch);

        JavaRDD<BranchLevelSalesData> branchAndStoreLevelSalesDataRDD = customerIdentifierVsAmountRDD
                .reduceByKey(BigDecimal::add)
                .map(row ->
                        new BranchLevelSalesData(row._1().getCustomerNumber(), row._1().getStoreNumber(), row._1().getBranch(), row._2()));

        CassandraConnector connector = CassandraConnector.apply(sparkConf);

        try (Session session = connector.openSession()) {

            session.execute("CREATE TABLE IF NOT EXISTS sales_data.branch_to_store_level_sales (customer_number text," +
                    " store_number TEXT, " +
                    "branch TEXT," +
                    "amount decimal," +
                    " PRIMARY KEY (customer_number,store_number))");
        }

        CassandraJavaUtil.javaFunctions(branchAndStoreLevelSalesDataRDD).writerBuilder("sales_data", "branch_to_store_level_sales", mapToRow(BranchLevelSalesData.class)).saveToCassandra();
        System.out.println("Finished processing........................" + LocalDateTime.now());
    }

    private CassandraJavaRDD<CassandraRow> readData(String keySpace, String table) {
        return CassandraJavaUtil
                .javaFunctions(javaSparkContext)
                .cassandraTable(keySpace, table);

    }

    private static BranchAndStoreLevelSales mapToBranch(Tuple2<Tuple2<String, String>, Tuple2<String, BigDecimal>> v1) {
        BranchAndStoreLevelSales.CustomerIdentifier identifier = new BranchAndStoreLevelSales.CustomerIdentifier(v1._1()._1(), v1._1()._2(), v1._2()._1());
        return new BranchAndStoreLevelSales(identifier, v1._2()._2());
    }

}
