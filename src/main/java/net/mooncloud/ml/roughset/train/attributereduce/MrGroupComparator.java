package net.mooncloud.ml.roughset.train.attributereduce;

import net.mooncloud.io.Tuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MrGroupComparator extends WritableComparator {

	/**
	 * 
	 */
	public MrGroupComparator() {
		super(Tuple.class, true);
	}

	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		Tuple t1 = (Tuple) o1;
		Tuple t2 = (Tuple) o2;
		WritableComparable key1 = ((WritableComparable) t1.get(0));
		WritableComparable key2 = ((WritableComparable) t2.get(0));
		WritableComparable flag1 = ((WritableComparable) t1.get(1));
		WritableComparable flag2 = ((WritableComparable) t2.get(1));
		return key1.equals(key2) ? (flag1.equals(flag2) ? 0 : flag1
				.compareTo(flag2)) : (key1.compareTo(key2));
	}
}