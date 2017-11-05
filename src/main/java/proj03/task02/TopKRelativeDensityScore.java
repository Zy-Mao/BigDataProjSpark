package proj03.task02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.sources.In;
import org.codehaus.janino.Java;
import scala.Serializable;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class TopKRelativeDensityScore {
	private static int K_NUM = 100;

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage: [input file path] [output file path]");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("TopKRelativeDensityScore0").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> linesRDD = sparkContext.textFile(args[0]).cache();

		// Read point data <line> and map into <cellID, 1>
		JavaPairRDD<Integer, Integer> densityPartPRDD = linesRDD.mapToPair(new PairFunction<String, Integer, Integer>() {
			@Override
			public Tuple2<Integer, Integer> call(String s) throws Exception {
				String stringSplits[] = s.replace("(", "").replace(")", "").split(",");
				return new Tuple2<Integer, Integer>(getGridCellId(
						Float.parseFloat(stringSplits[0]), Float.parseFloat(stringSplits[1])), 1);
			}
			private int getGridCellId(float x, float y) {
				return (int) ((x - 1) / 20 + ((10000 - y) / 20) * 500);
			}
		});

		// Reduce <cellID, count>
		JavaPairRDD<Integer, Integer> densityPRDD = densityPartPRDD.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		// Map <cellID, count> to <cellID, "SelfDensity_NeighborDensitySum_NeighborCount">
		JavaPairRDD<Integer, String> relDensityStrPartPRDD = densityPRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Integer, Integer>, Integer, String>() {
			@Override
			public Iterator<Tuple2<Integer, String>> call(Tuple2<Integer, Integer> currTuple) throws Exception {
				int cellId = currTuple._1();
				int count = currTuple._2();
				List<Tuple2<Integer, String>> tupleList = new ArrayList<>();
				tupleList.add(new Tuple2<>(cellId, count + "_0_0"));
				for (int i = (cellId % 500 == 1 ? 0 : -1); i <= (cellId % 500 == 0 ? 0 : 1); i++) {
					for (int j = (cellId <= 500 ? 0 : -1); j <= (cellId >= 24950 ? 0 : 1); j++) {
						if (i == 0 && j == 0) {
							continue;
						}
						int neighborId = cellId + i + j * 500;
						tupleList.add(new Tuple2<>(neighborId, "0_" + count + "_1"));
					}
				}
				return tupleList.iterator();
			}
		});

		// Reduce <cellID, "SelfDensity_NeighborDensitySum_NeighborCount">
		JavaPairRDD<Integer, String> relDensityStrPRDD = relDensityStrPartPRDD.reduceByKey(
				new Function2<String, String, String>() {
			@Override
			public String call(String s1, String s2) throws Exception {
				String[] s1Splits = s1.split("_");
				String[] s2Splits = s2.split("_");
				int selfDensity = Integer.parseInt(s1Splits[0]) + Integer.parseInt(s2Splits[0]);
				int neighborDensitySum = Integer.parseInt(s1Splits[1]) + Integer.parseInt(s2Splits[1]);
				int neighborCount = Integer.parseInt(s1Splits[2]) + Integer.parseInt(s2Splits[2]);
				return  selfDensity + "_" + neighborDensitySum + "_" + neighborCount;
			}
		});

		// Map <cellID, "SelfDensity_NeighborDensitySum_NeighborCount"> to <RelativeDensity, cellID>
		JavaPairRDD<Float, Integer> relDensitySwapPRDD = relDensityStrPRDD.mapToPair(
				new PairFunction<Tuple2<Integer, String>, Float, Integer>() {
			@Override
			public Tuple2<Float, Integer> call(Tuple2<Integer, String> currTuple) throws Exception {
				String[] splits = currTuple._2().split("_");
				if (Integer.parseInt(splits[2]) == 0 || Integer.parseInt(splits[1]) == 0) {
//					System.out.println("Zero found: " + currTuple._1() + ", " + currTuple._2());
					return new Tuple2<>((float) 0.0, currTuple._1());
				}
				float relDensity = Integer.parseInt(splits[0]) / (Integer.parseInt(splits[1]) / Integer.parseInt(splits[2]));
				return new Tuple2<>(relDensity, currTuple._1());
			}
		});

		// Sort <RelativeDensity, cellID> by key, and get k number from the front of the collection
		List<Tuple2<Float, Integer>> topKRelDensitySwapList = relDensitySwapPRDD.sortByKey(false).take(K_NUM);
		JavaPairRDD<Integer, Float> topKRelDensityPRDD = sparkContext.parallelizePairs(topKRelDensitySwapList)
				.mapToPair(new PairFunction<Tuple2<Float, Integer>, Integer, Float>() {
					@Override
					public Tuple2<Integer, Float> call(Tuple2<Float, Integer> currTuple) throws Exception {
						return currTuple.swap();
					}
				});

		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US)
				.format(Calendar.getInstance().getTime());
		topKRelDensityPRDD.saveAsTextFile(args[1] + "/P2_B/" + timeStamp);

		

		sparkContext.close();
	}
}