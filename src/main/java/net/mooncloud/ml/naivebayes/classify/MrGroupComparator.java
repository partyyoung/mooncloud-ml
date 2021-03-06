package net.mooncloud.ml.naivebayes.classify;

import net.mooncloud.io.Tuple;

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
		return key1.compareTo(key2);
	}
}