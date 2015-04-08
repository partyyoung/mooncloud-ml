package net.mooncloud.ml.naivebayes.classify;

import net.mooncloud.io.Tuple;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MrPartitioner extends Partitioner<Tuple, Tuple>
{
	@Override
	public int getPartition(Tuple key, Tuple value, int numPartitions)
	{
		return (((Text) key.get(0)).hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}