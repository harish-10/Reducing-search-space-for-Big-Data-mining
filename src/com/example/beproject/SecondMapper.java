package com.example.beproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SecondMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		List<String> SingletonsList1 = MapReduceDriver.getList();
		List<ArrayList<String>> list1 = null;
		String line = ivalue.toString().trim();
		String[] tokens;
		tokens = line.trim().split(" ");
		int flag = 0;
		List<String> l = null;
		flag = 0;
		for (String val : SingletonsList1) {

			list1 = new ArrayList<ArrayList<String>>();
			l = new ArrayList<String>();
			for (int i = 0; i < tokens.length; i++) {
				String[] tokens1 = tokens[i].split(":");
				if (val.equals(tokens1[0])) {
					l.add(tokens[i]);
					flag = 1;
				} else {
					if (SingletonsList1.contains(tokens1[0]))
						l.add(tokens[i]);
				}

			}
		}
		if (flag == 1 && l.size() > 1) {
			list1.add(new ArrayList<String>(l));
			context.write(new IntWritable(1), new Text(list1.toString()));
		}
	}
}
