package proj03.task02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountDemo {
	public static void main(String[] args) {
//		SparkConf sparkConf = new SparkConf().setAppName("Example Spark App").setMaster("local[*]");  // Delete this line when submitting to a cluster
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Count");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> linesRDD = sparkContext.textFile("src/main/resources/test.data").cache();
		System.out.println("Number of lines in file = " + linesRDD.count());

//		JavaRDD<String> lines = sc.textFile("hdfs://hadoop:9000/test/spark.txt");
//		JavaRDD<String> lines = sc.textFile("src/main/resources/test.data");
//
////		String logFile = "file:///opt/spark-2.1.0-bin-hadoop2.7/README.md"; // Should be some file on your system
//
//		JavaPairRDD<String, Integer> counts = lines
//				.flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
//				.mapToPair(word -> new Tuple2<>(word, 1))
//				.reduceByKey((a, b) -> a + b);
//		counts.foreach(p -> System.out.println(p));
//		System.out.println("Total words: " + counts.count());
//		counts.saveAsTextFile("/tmp/shakespeareWordCount");
//
		JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" ")).iterator();
			}
		});

//		JavaRDD<String> words = linesRDD.flatMap(new FlatMapFunction<String, String>() {
//			@Override
//			public Iterable<String> call(String line) throws Exception {
//				return Arrays.asList(line.split(" "));
//			}
//		});
//

		JavaPairRDD<String, Integer> pairsRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> wordsCountsPairRDD = pairsRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		wordsCountsPairRDD.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple._1+":"+tuple._2);
			}
		});

		wordsCountsPairRDD.saveAsTextFile("src/main/resources/output");

		sparkContext.close();

	}
}
