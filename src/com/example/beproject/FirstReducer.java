package com.example.beproject;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {

	public void reduce(IntWritable _key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {

		float total = 0;
		for (FloatWritable val : values) {
			total += val.get();
		}
		if (total >= MapReduceDriver.minSup) {
			MapReduceDriver.tempMap.put(_key.get(), total);
			context.write(_key, new FloatWritable(total));

		}
	}

}
