package proj03.task02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
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

		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US)
				.format(Calendar.getInstance().getTime());
		SparkConf sparkConf = new SparkConf().setAppName("TopKRelativeDensityScore0").setMaster("local");

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> linesRDD = sparkContext.textFile(args[0]);

		// Read point data <line> and map into <cellID, 1>
		JavaPairRDD<Integer, Integer> densityPartPRDD = linesRDD.mapToPair(new PairFunction<String, Integer, Integer>() {
			@Override
			public Tuple2<Integer, Integer> call(String s) throws Exception {
				String stringSplits[] = s.replace("(", "").replace(")", "").split(",");
				return new Tuple2<>(getGridCellId(
						Float.parseFloat(stringSplits[0]), Float.parseFloat(stringSplits[1])), 1);
			}

			private int getGridCellId(float x, float y) {
				return ((int) x - 1) / 20 + 1 + ((10000 - (int) y) / 20) * 500;
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

		// Map <cellID, count> to <cellID, "SelfCount_NeighborTotalCount">
		JavaPairRDD<Integer, String> relDensityStrPartPRDD = densityPRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Integer, Integer>, Integer, String>() {
					@Override
					public Iterator<Tuple2<Integer, String>> call(Tuple2<Integer, Integer> currTuple) throws Exception {
						int cellId = currTuple._1();
						int count = currTuple._2();
						List<Tuple2<Integer, String>> tupleList = new ArrayList<>();
						tupleList.add(new Tuple2<>(cellId, count + "_0"));
						for (int i = (cellId % 500 == 1 ? 0 : -1); i <= (cellId % 500 == 0 ? 0 : 1); i++) {
							for (int j = (cellId <= 500 ? 0 : -1); j <= (cellId >= 249501 ? 0 : 1); j++) {
								if (i == 0 && j == 0) {
									continue;
								}
								int neighborId = cellId + i + j * 500;
								tupleList.add(new Tuple2<>(neighborId, "0_" + count));
							}
						}
						return tupleList.iterator();
					}
				});

		// Reduce <cellID, "SelfCount_NeighborTotalCount">
		JavaPairRDD<Integer, String> relDensityStrPRDD = relDensityStrPartPRDD.reduceByKey(
				new Function2<String, String, String>() {
					@Override
					public String call(String s1, String s2) throws Exception {
						String[] s1Splits = s1.split("_");
						String[] s2Splits = s2.split("_");
						int selfCount = Integer.parseInt(s1Splits[0]) + Integer.parseInt(s2Splits[0]);
						int neighborTotalCount = Integer.parseInt(s1Splits[1]) + Integer.parseInt(s2Splits[1]);
						return selfCount + "_" + neighborTotalCount;
					}
				});

		// Map <cellID, "SelfDensity_NeighborDensitySum"> to <RelativeDensity, cellID>
		JavaPairRDD<Float, Integer> relDensitySwapPRDD = relDensityStrPRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Integer, String>, Float, Integer>() {
					@Override
					public Iterator<Tuple2<Float, Integer>> call(Tuple2<Integer, String> currTuple) throws Exception {
						List<Tuple2<Float, Integer>> list = new ArrayList<>();
						String[] splits = currTuple._2().split("_");
						if (Integer.parseInt(splits[1]) == 0) {
							return list.iterator();
						}
						int cellID = currTuple._1();
						float neighborNum = ((cellID % 500 == 0 || cellID % 500 == 1) ? 2 : 3) *
								((cellID <= 500 || cellID >= 249501) ? 2 : 3) - 1;
						float relDensity = Float.parseFloat(splits[0])
								/ (Float.parseFloat(splits[1]) / neighborNum);
						list.add(new Tuple2<>(relDensity, cellID));
						return list.iterator();
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
				}).cache();


		topKRelDensityPRDD.saveAsTextFile(args[1] + "/" + timeStamp + "/P2_B");

		System.out.println("============ Problem 2 - C ============");

		// Map topKRelDensityPRDD<RelativeDensity, cellID> to <neighborID, cellID>
		JavaPairRDD<Integer, Integer> neighborPRDD = topKRelDensityPRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Integer, Float>, Integer, Integer>() {
					@Override
					public Iterator<Tuple2<Integer, Integer>> call(Tuple2<Integer, Float> currTuple) throws Exception {
						int cellId = currTuple._1();
						List<Tuple2<Integer, Integer>> tupleList = new ArrayList<>();
						for (int i = (cellId % 500 == 1 ? 0 : -1); i <= (cellId % 500 == 0 ? 0 : 1); i++) {
							for (int j = (cellId <= 500 ? 0 : -1); j <= (cellId >= 24950 ? 0 : 1); j++) {
								if (i == 0 && j == 0) {
									continue;
								}
								int neighborId = cellId + i + j * 500;
								tupleList.add(new Tuple2<>(neighborId, cellId));
							}
						}
						return tupleList.iterator();
					}
				});

		// Swap relDensitySwapPRDD<RelativeDensity, cellID>, generate relDensityPRDD<cellID, RelativeDensity>
		JavaPairRDD<Integer, Float> relDensityPRDD = relDensitySwapPRDD.mapToPair(
				new PairFunction<Tuple2<Float, Integer>, Integer, Float>() {
					@Override
					public Tuple2<Integer, Float> call(Tuple2<Float, Integer> currTuple) throws Exception {
						return currTuple.swap();
					}
				}).cache();

		// Join topKGridCellNeighborPRDD<neighborID, cellID> with relDensityPRDD<cellID, RelativeDensity>
		// generate topKNeighborScoreJoinPRDD<neighborID, <cellID, RelativeDensity>>
		JavaPairRDD<Integer, Tuple2<Integer, Float>> topKNeighborScoreJoinPRDD
				= neighborPRDD.join(relDensityPRDD);

		// Map topKNeighborScoreJoinPRDD<neighborID, <cellID, RelativeDensity>>
		// into tmpTopKNeighborScoreJoinPRDD<cellID, "{neighborID:RelativeDensity}">
		JavaPairRDD<Integer, String> tmpTopKNeighborScoreJoinPRDD = topKNeighborScoreJoinPRDD.mapToPair(
				new PairFunction<Tuple2<Integer, Tuple2<Integer, Float>>, Integer, String>() {
					@Override
					public Tuple2<Integer, String> call(Tuple2<Integer, Tuple2<Integer, Float>> currTuple) throws Exception {
						int cellID = currTuple._2()._1();
						int neighborID = currTuple._1();
						float relativeDensity = currTuple._2()._2();
						return new Tuple2<>(cellID, "{" + neighborID + ":" + relativeDensity + "}");
					}
				});

		// Reduce tmpTopKNeighborScoreJoinPRDD<cellID, "{neighborID:RelativeDensity}">
		JavaPairRDD<Integer, String> topKNeighborScorePRDD = tmpTopKNeighborScoreJoinPRDD.reduceByKey(
				new Function2<String, String, String>() {
					@Override
					public String call(String s1, String s2) throws Exception {
						return s1 + ";" + s2;
					}
				});

		topKNeighborScorePRDD.repartition(1).saveAsTextFile(args[1] + "/" + timeStamp + "/P2_C");

		sparkContext.close();
	}
}