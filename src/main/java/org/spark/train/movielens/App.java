package org.spark.train.movielens;

import java.awt.Color;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.JFrame;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.math.plot.Plot2DPanel;

import scala.Tuple2;

public class App {
	public static void main(String[] args) {
		test();
	}

	public static void test() {
		SparkConf sparkConf = new SparkConf().setMaster("local")
				.setAppName("CS 105 lab2");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("ERROR");
		JavaRDD<String> userData = sc.textFile("ml-100k/u.user");
		assert (userData.first() == "1|24|M|technician|85711");// always check the
																														// RDD is as
																														// expected
		// System.out.println(userData.first());
		JavaRDD<String[]> userFields = userData.map(line -> line.split("\\|"));
		long numUsers = userFields.map(fields -> fields[0]).count();
		long numGenders = userFields.map(fields -> fields[2]).distinct().count();
		long numOccupations = userFields.map(fields -> fields[3]).distinct()
				.count();
		long numZipCodes = userFields.map(fields -> fields[4]).distinct().count();
		System.out.println(userFields.first()[2]);

		System.out.println("Users: " + numUsers + ", genders: " + numGenders
				+ ", occupations: " + numOccupations + ", zip codes: " + numZipCodes);

		List<Double> ages = userFields.map(line -> Double.parseDouble(line[1]))
				.collect();
//		double maxAge = Collections.max(ages);
		double[] agesArray = new double[ages.size()];
		int i = 0;
		for (Double age : ages) {
			agesArray[i++] = Double.valueOf(age);
		}

		Plot2DPanel plot = new Plot2DPanel();
		// System.out.println(agesArray[0]);
		// plot.addHistogramPlot("tt", Color.BLUE, agesArray, 20);

		List<Tuple2<String, Integer>> countByOccupations = userFields
				.mapToPair(fields -> new Tuple2<String, Integer>(fields[3], 1))
				.reduceByKey((x, y) -> x + y).collect();
		String[] xVals = new String[countByOccupations.size()];
		int[] yVals = new int[countByOccupations.size()];
		List<String> xAxis = new ArrayList<String>();
		List<Integer> yAxis = new ArrayList<Integer>();
		for (Tuple2<String, Integer> val: countByOccupations){
			xAxis.add(val._1());
			yAxis.add(val._2());
		}

		Collections.sort(xAxis);
		Collections.sort(yAxis);
		for (i = 0; i < countByOccupations.size(); i++){
			xVals[i] = xAxis.get(i);
			yVals[i] = yAxis.get(i);
		}

//		System.out.println(countByOccupations.get(0));
//		JFrame frame = new JFrame("Hist panel");
//		frame.setContentPane(plot);
//		frame.setVisible(true);

		sc.close();

	}
}
