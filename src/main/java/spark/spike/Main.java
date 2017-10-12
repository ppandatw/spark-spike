package spark.spike;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Date;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static java.time.LocalDate.parse;
import static java.time.ZoneId.systemDefault;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.time.temporal.TemporalAdjusters.firstDayOfMonth;

public class Main {

    private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
        .appendValue(YEAR, 4)
        .appendValue(MONTH_OF_YEAR, 2)
        .appendValue(DAY_OF_MONTH, 2)
        .toFormatter();

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
            .setMaster("local")
            .setAppName("abcd");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sparkContext
            .textFile("/Users/ritabratamoitra/Projects/Learning/Spark-Spike/spark-spike/src/main/resources/MDW_hcmrevenue_v001_20160531_20160602_024440.csv", 0);

        String first = rdd.first();
        rdd.filter(row -> !row.equals(first))
            .map(Main::func)
            .take(10)
            .forEach(System.out::println);
    }

    private static SalesData func(String v1) {
        String[] split = v1.split("\t");

        return new SalesData().builder()
            .customer_Number(split[1])
            .store_Number(split[2])
            .mtd_Date(toFirstDayOfMonth(removeQuotes(split[4])))
            .department(Integer.parseInt(removeQuotes(split[7])))
            .invoice_type(Integer.parseInt(removeQuotes(split[8])))
            .mtdAmount(BigDecimal.valueOf(Long.parseLong(removeQuotes(split[8]))))
            .build();
    }

    private static String removeQuotes(String s) {
        return s.replaceAll("\\\"", "");
    }

    private static Date toFirstDayOfMonth(String dateAsString) {
        ZonedDateTime beginOfMonth = toZonedDateTime(dateAsString).with(firstDayOfMonth());
        return Date.from(beginOfMonth.toInstant());
    }

    private static ZonedDateTime toZonedDateTime(String dateAsString) {
        return parse(dateAsString, DATE_FORMATTER)
            .atStartOfDay(systemDefault());
    }

}
