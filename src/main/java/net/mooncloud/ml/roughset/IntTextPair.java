package net.mooncloud.ml.roughset;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntTextPair extends WritableComparator implements WritableComparable<IntTextPair>
{
	private IntWritable first;
	private Text second;

	public IntTextPair()
	{
		super(IntTextPair.class);
		set(new IntWritable(), new Text());
	}

	public IntTextPair(int first, String second)
	{
		super(TextPair.class);
		set(new IntWritable(first), new Text(second));
	}

	public IntTextPair(IntWritable first, Text second)
	{
		super(IntTextPair.class);
		set(first, second);
	}

	private void set(IntWritable first, Text second)
	{
		// TODO Auto-generated method stub
		this.first = first;
		this.second = second;
	}

	public IntWritable getFirst()
	{
		return first;
	}

	public Text getSecond()
	{
		return second;
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);
	}

	@Override
	public int hashCode()
	{
		return first.hashCode() * 163 + second.hashCode();
	}

	public boolean equals(Object o)
	{
		if (o instanceof IntTextPair)
		{
			IntTextPair tp = (IntTextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}

	@Override
	public String toString()
	{
		return first + "\t" + second;
	}

	@Override
	public int compareTo(IntTextPair o)
	{
		// TODO Auto-generated method stub
		int cmp = first.compareTo(o.first);
		if (cmp != 0)
			return cmp;
		return second.compareTo(o.second);
	}

}
