package net.mooncloud.ml.naivebayes.classify;

import java.io.IOException;
import java.util.HashMap;

import net.mooncloud.Record;
import net.mooncloud.mapreduce.TableReducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class MrReducer extends TableReducer<IntWritable, Record> {

	private Record res = null;
	private String finalltarget;

	private HashMap<String, Double> targetprobability = new HashMap<String, Double>();
	private HashMap<String, Double> featureprobability = new HashMap<String, Double>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		res = new Record(context.getConfiguration().get("mapred.output.schema"));
	}

	@Override
	public void reduce(IntWritable key, Iterable<Record> values, Context context)
			throws IOException, InterruptedException {
		int flag = key.get();

		if (0 == flag) {
			for (Record record : values) {
				targetprobability.put(record.get("target").toString(),
						((DoubleWritable) record.get("probability")).get());
			}
		} else if (1 == flag) {
			for (Record record : values) {
				featureprobability.put(record.get("target").toString() + "\001"
						+ record.get("feature").toString() + "\001"
						+ record.get("feature_value").toString(),
						((DoubleWritable) record.get("probability")).get());
			}
		} else {
			for (Record record : values) {
				double max_pro = 0.0;
				String outlook = record.get("outlook").toString();
				String temperature = record.get("temperature").toString();
				String humidity = record.get("humidity").toString();
				String windy = record.get("windy").toString();

				for (String target : targetprobability.keySet()) {
					try {
						double pro = targetprobability.get(target)
								* featureprobability.get(target
										+ "\001outlook\001" + outlook)
								* featureprobability.get(target
										+ "\001temperature\001" + temperature)
								* featureprobability.get(target
										+ "\001humidity\001" + humidity)
								* featureprobability.get(target
										+ "\001windy\001" + windy);
						if (pro > max_pro) {
							max_pro = pro;
							finalltarget = target;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.out.println(target + "\t" + max_pro + "\t"
							+ record.toString(","));
				}

				res.set("outlook", record.get("outlook"));
				res.set("temperature", record.get("temperature"));
				res.set("humidity", record.get("humidity"));
				res.set("windy", record.get("windy"));
				res.set("play", new Text(finalltarget));
				context.write(res, NullWritable.get());
			}
		}

	}
}
