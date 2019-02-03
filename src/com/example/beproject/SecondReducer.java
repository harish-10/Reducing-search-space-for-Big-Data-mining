package com.example.beproject;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<IntWritable, Text, Text, Text> {

	public void reduce(IntWritable _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// process values
		for (Text val : values) {
			String line = val.toString().substring(2, val.toString().length() - 2);
			String[] tokens = line.trim().split(",");
			Iterator<String> itr = MapReduceDriver.Singletonslist.iterator();
			String[] tempArr = new String[tokens.length];
			int j = 0;
			while (itr.hasNext()) {
				String element = itr.next();
				for (int i = 0; i < tokens.length; i++) {
					String[] split = tokens[i].trim().split(":");
					if (element.equals(split[0].trim())) {
						tempArr[j] = tokens[i];
						j++;
					}
				}
			}
			for (int i = 0; i < tokens.length; i++) {
				tokens[i] = tempArr[i].trim();
			}
			String[] tokens1;
			String[] tokens2 = {};
			for (int i = 0; i < tokens.length; i++) {
				tokens1 = tokens[i].trim().split(":");

				String key = tokens1[0].trim();

				LinkedList<String> valList;

				Float supValue = (float) 0;

				for (j = i + 1; j < tokens.length; j++) {
					valList = new LinkedList<>();
					tokens2 = tokens[j].trim().split(":");
					supValue = MapReduceDriver.round(Float.parseFloat(tokens1[1]) * Float.parseFloat(tokens2[1]));

					int flag = 0;

					if (MapReduceDriver.map.get(key) != null) {
						valList = MapReduceDriver.map.get(key);
						Iterator<String> s = valList.iterator();
						while (s.hasNext()) {
							String temp = s.next().trim();
							if (temp.split(":")[0].equals(tokens2[0].trim())) {
								String[] split = temp.trim().split(":");
								supValue = supValue + Float.parseFloat(split[1]);
								valList.set(valList.indexOf(temp), tokens2[0] + ":" + supValue.toString());
								flag = 1;
							}
						}

					}
					if (flag == 0) {
						valList.add((tokens2[0].trim() + ":" + supValue.toString().trim()));
					}
					MapReduceDriver.map.put(key, valList);
				}
			}
		}
		MapReduceDriver.map.values().remove(null);

		Set<String> keys = MapReduceDriver.map.keySet();
		for (String key : keys) {
			if (MapReduceDriver.map.get(key) != null) {
				LinkedList<String> lvalue = MapReduceDriver.map.get(key);
				for (int i = 0; i < lvalue.size(); i++) {
					String llisti = lvalue.get(i);
					String newKeyi = key;
					String[] ltoken = llisti.trim().split(":");
					newKeyi = newKeyi + "," + ltoken[0];
					putInMap(newKeyi, Float.parseFloat(ltoken[1]));
				}

				for (int i = 0; i < lvalue.size(); i++) {
					String llisti = lvalue.get(i);
					String newKeyi = key;
					String[] ltoken = llisti.trim().split(":");
					newKeyi = newKeyi + "," + ltoken[0];
					for (int j = i + 1; j < lvalue.size(); j++) {
						String newKeyj;
						String llistj = lvalue.get(j);
						String[] ltoken1 = llistj.trim().split(":");
						newKeyj = newKeyi + "," + ltoken1[0];
						float tval = getSupVal(newKeyj, Float.parseFloat(ltoken[1]));
						if (Float.parseFloat(ltoken[1]) <= tval)
							putInMap(newKeyj, Float.parseFloat(ltoken[1]));
						else
							putInMap(newKeyj, tval);

					} // end of j loop

				}
				String newKeyi = key;
				float min = 999;
				String[] ltoken = null;
				for (int i = 0; i < lvalue.size(); i++) {
					String llisti = lvalue.get(i);
					ltoken = llisti.trim().split(":");
					newKeyi = newKeyi + "," + ltoken[0];
					float val = getSupVal(newKeyi, min);
					if (!MapReduceDriver.finalMap.containsKey(newKeyi)) {
						putInMap(newKeyi, val);
					}
				}
			}
		}
		keys = MapReduceDriver.finalMap.keySet();
		for (String key : keys) {
			float total = MapReduceDriver.round(MapReduceDriver.finalMap.get(key));
			if (total >= MapReduceDriver.minSup)
				context.write(new Text(key.trim()), new Text(Float.toString(total)));
		}

	}

	private float getSupVal(String newKeyi, float min) {
		// TODO Auto-generated method stub

		String[] temp = newKeyi.trim().split(",");
		for (int i = 0; i < temp.length & temp.length > 2; i++) {
			for (int j = i + 1; j < temp.length; j++) {
				String temp1 = temp[i].trim() + "," + temp[j].trim();
				if (MapReduceDriver.finalMap.containsKey(temp1)) {
					if (min > MapReduceDriver.finalMap.get(temp1)) {
						min = MapReduceDriver.finalMap.get(temp1);
					}
				}
			}
		}
		return min;
	}

	public static void putInMap(String newKey, float val) {
		if (!MapReduceDriver.finalMap.containsKey(newKey)) {
			MapReduceDriver.finalMap.put(newKey, val);
		} else {
			MapReduceDriver.finalMap.computeIfPresent(newKey, (k, v) -> val);
		}
	}

}
