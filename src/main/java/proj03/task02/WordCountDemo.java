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


		// 如果要在spark集群上运行，需要修改的，只有两个地方
		// 第一，将SparkConf的setMaster()方法给删掉，默认它自己会去连接
		// 第二，我们针对的不是本地文件了，修改为hadoop hdfs上的真正的存储大数据的文件

		// 实际执行步骤：
		// 1、将spark.txt文件上传到hdfs上去
		// 2、使用我们最早在pom.xml里配置的maven插件，对spark工程进行打包
		// 3、将打包后的spark工程jar包，上传到机器上执行
		// 4、编写spark-submit脚本
		// 5、执行spark-submit脚本，提交spark应用到集群执行

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
