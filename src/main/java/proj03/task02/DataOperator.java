package proj03.task02;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;

public class DataOperator {
	private static String POINT_FILE_NAME = "Point.data";

	public class Point {
		private int x;
		private int y;

		public Point() {}

		@Override
		public String toString() {
			return x + "," + y;
		}

		public int getX() {
			return x;
		}

		public void setX(int x) {
			this.x = x;
		}

		public int getY() {
			return y;
		}

		public void setY(int y) {
			this.y = y;
		}
	}

	private void generatePoint() {
		int count = 0;
		ArrayList<Point> pointArrayList = new ArrayList<>();
		while (true) {
			count += 1;
			Point point = new Point();
			point.setX(generateRandomInt(1, 10000));
			point.setY(generateRandomInt(1, 10000));
			pointArrayList.add(point);

			if (count % 500000 == 0) {
				outputDataIntoFiles(pointArrayList, POINT_FILE_NAME);
				pointArrayList = new ArrayList<>();
				System.out.println("Generated and output " + count + " Point records.");
				if (getFileSize(POINT_FILE_NAME) >= 100*1024*1024) {
					break;
				}
			}

		}
		System.out.println("Finished: generated and output Point records.");
	}

	// generate a random number in range [min, max]
	private int generateRandomInt(int min, int max) {
		return new Random().nextInt(max - min + 1) + min;
	}

	private long getFileSize(String fileName) {
		File file = new File(fileName);
		return (!file.exists() || !file.isFile()) ? 0 : file.length();
	}

	// Output the data in the input ArrayList into the file, line by line.
	private void outputDataIntoFiles(ArrayList arrayList, String fileName) {
		FileWriter fileWriter;
		BufferedWriter bufferedWriter;
		PrintWriter printWriter = null;
		StringBuilder strBuilder = new StringBuilder();

		try {
			fileWriter = new FileWriter(fileName, true);
			bufferedWriter = new BufferedWriter(fileWriter);
			printWriter = new PrintWriter(bufferedWriter);
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (Object object : arrayList) {
			strBuilder.append(object.toString());
			strBuilder.append(System.lineSeparator());
		}
		try {
			printWriter.write(strBuilder.toString());
			printWriter.close();
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws Exception {
		new DataOperator().generatePoint();
	}
}
