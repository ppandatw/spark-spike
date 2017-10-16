package spark.spike;

import java.awt.SecondaryLoop;
import java.util.Properties;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Serializable;
import scala.Tuple2;

public class BranchImporter implements Serializable{

    public JavaPairRDD<CustomerIdentifier, String> importBranchMapping(SQLContext sqlContext) {
        JavaRDD<Row> javaRDD = sqlContext
            .read()
            .option("fetchSize", "1000")
            .jdbc("jdbc:postgresql://postgres:5432/testDB?user=test&password=test",
                "portfolio",
                new Properties() {{
                    setProperty("driver", "org.postgresql.Driver");
                }}
            ).javaRDD();

        return javaRDD
            .mapToPair((PairFunction<Row, CustomerIdentifier, String>) row -> new Tuple2<>(new CustomerIdentifier(row.getAs("store_number"), row.getAs("customer_number")),
                row.getAs("metro_branch")));
    }
}
