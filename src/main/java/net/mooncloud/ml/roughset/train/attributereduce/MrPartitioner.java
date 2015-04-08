package net.mooncloud.ml.roughset.train.attributereduce;

import net.mooncloud.io.Tuple;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MrPartitioner extends Partitioner<Tuple, Tuple>
{
	@Override
	public int getPartition(Tuple key, Tuple value, int numPartitions)
	{
		return (((WritableComparable) key.get(0)).hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}