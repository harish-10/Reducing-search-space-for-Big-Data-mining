package com.example.beproject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.Stopwatch;

public class MapReduceDriver {

	public static List<Integer> list = new ArrayList<Integer>();
	public static float minSup;
	public static List<String> Singletonslist = new ArrayList<String>();
	public static Map<Integer, Float> tempMap = new HashMap<>();
	public static Map<String, LinkedList<String>> map = new HashMap<>();
	public static Map<String, Float> finalMap = new HashMap<>();
	public static List<Integer> tempList = new ArrayList<Integer>();

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		Stopwatch timer = new Stopwatch().start();
		String inputPath;
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "frequent patterns");
		job1.setJarByClass(com.example.beproject.MapReduceDriver.class);
		// TODO: specify a mapper
		job1.setMapperClass(FirstMapper.class);
		// TODO: specify a reducer
		job1.setReducerClass(FirstReducer.class);

		// TODO: specify mapper output types
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(FloatWritable.class);
		// TODO: specify reducer output types
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(FloatWritable.class);

		String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
		int i = 0;
		inputPath = otherArgs[0];
	/*	int m = Integer.parseInt(otherArgs[1]);
		for (i = 2; i < m + 2; i++) {
			list.add(Integer.parseInt(otherArgs[i]));
		}
		minSup = Float.parseFloat(otherArgs[i]);
		*/
		minSup=Float.parseFloat(otherArgs[1]);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path("/output/mapperOut"));

		if (!job1.waitForCompletion(true))
			return;

		Map<Integer, Float> tempSortedMap = sortByComparator(tempMap, false);
		Iterator<Integer> itr = tempList.iterator();
		while (itr.hasNext()) {
			Singletonslist.add(itr.next().toString());
		}

		Job job2 = Job.getInstance(conf, "frequent patterns");
		job2.setJarByClass(com.example.beproject.MapReduceDriver.class);

		// TODO: specify a mapper
		job2.setMapperClass(SecondMapper.class);
		// TODO: specify a reducer
		job2.setReducerClass(SecondReducer.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		// TODO: specify output types
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		Iterator<String> itr1 = MapReduceDriver.Singletonslist.iterator();
		while (itr1.hasNext()) {
			map.put(itr1.next(), null);
		}

		FileInputFormat.setInputPaths(job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(job2, new Path("/output/mapper2Out"));

		if (!job2.waitForCompletion(true))
			return;

		timer.stop();
		System.out.println(timer.elapsedMillis());
		long stopTime = System.currentTimeMillis();
		System.out.println("Elapsed Time :" + (stopTime - startTime));

	}

	public static Map<Integer, Float> sortByComparator(Map<Integer, Float> unsortMap, final boolean order) {
		List<Map.Entry<Integer, Float>> list = new LinkedList<Map.Entry<Integer, Float>>(unsortMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<Integer, Float>>() {

			@Override
			public int compare(Map.Entry<Integer, Float> o1, Map.Entry<Integer, Float> o2) {
				// TODO Auto-generated method stub
				if (order)
					return (o1.getValue()).compareTo(o2.getValue());
				else
					return (o2.getValue()).compareTo(o1.getValue());
			}

		});
		Map<Integer, Float> sortedMap = new LinkedHashMap<Integer, Float>();

		for (Map.Entry<Integer, Float> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
			tempList.add(entry.getKey());
		}
		return sortedMap;

	}

	public static float round(double value) {
		BigDecimal bd = new BigDecimal(value);
		bd = bd.setScale(2, RoundingMode.HALF_UP);
		return bd.floatValue();
	}

	public static List<String> getList() {
		return Singletonslist;
	}
}
