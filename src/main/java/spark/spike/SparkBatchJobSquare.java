package spark.spike;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SparkBatchJobSquare {
	
	private JavaStreamingContext sparkStreamingContext;
	
	public SparkBatchJobSquare(SparkConf sparkConf, int durationInMillis) {
		sparkStreamingContext = new JavaStreamingContext(sparkConf, new Duration(durationInMillis));
	}
	
	public void run() {
		prepareSteps();
		sparkStreamingContext.start();
		System.out.println("Job started");
		sparkStreamingContext.awaitTermination();
	}
	
	private void prepareSteps() {
		List<Integer> list = IntStream
									 .range(1, 1000)
									 .boxed()
									 .collect(Collectors.toList());
		
		Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();
		for (int i = 0; i < 30; i++) {
			JavaRDD<Integer> parallelizedStream = sparkStreamingContext.sparkContext().parallelize(list);
			rddQueue.add(parallelizedStream);
		}
		
		// Create the QueueInputDStream and use it do some processing
		JavaDStream<Integer> inputStream = sparkStreamingContext.queueStream(rddQueue);
		
		JavaPairDStream<Integer, Integer> mappedStream = inputStream.mapToPair(
				i -> new Tuple2<>(i, i * i));
		
		AtomicReference<Tuple2<Integer, Integer>> capture = new AtomicReference<>();
		
		mappedStream.filter(pair -> {
			System.out.println(pair);
			return true;
		});
		
		mappedStream.print(5);
		mappedStream.foreachRDD(javaPairRDD -> {
			javaPairRDD.collect();
			return null;
			
		});
	}
	
	static void print(JavaPairRDD<Integer, Integer> pairRDD) {
	
	}
}
