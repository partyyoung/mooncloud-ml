package net.mooncloud.ml.naivebayes.train.featureprobability;

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
		Text key1 = ((Text) t1.get(0));
		Text key2 = ((Text) t2.get(0));
		return key1.compareTo(key2);
	}
}