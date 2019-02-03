package com.example.beproject;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

		String line = ivalue.toString();

		String[] tokens = line.trim().split(" ");
		List<Integer> list = MapReduceDriver.list;
		for (int i = 0; i < tokens.length; i++) {
			String[] tokens1 = tokens[i].trim().split(":");
			if (tokens1.length == 2) {
				if (!tokens1[0].isEmpty() & !tokens1[1].isEmpty())
			//		if (list.contains(Integer.parseInt(tokens1[0]))) {
						context.write(new IntWritable(Integer.parseInt(tokens1[0])),
								new FloatWritable(Float.parseFloat(tokens1[1])));
			//		}
			}
		}
	}

}
