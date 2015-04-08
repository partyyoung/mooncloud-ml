package net.mooncloud.ml.roughset;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextLongPair extends WritableComparator implements WritableComparable<TextLongPair>
{
	private Text first;
	private LongWritable second;

	public TextLongPair()
	{
		super(TextLongPair.class);
		set(new Text(), new LongWritable());
	}

	public TextLongPair(String first, long second)
	{
		super(TextLongPair.class);
		set(new Text(first), new LongWritable(second));
	}

	public TextLongPair(Text first, LongWritable second)
	{
		super(TextLongPair.class);
		set(first, second);
	}

	private void set(Text first, LongWritable second)
	{
		// TODO Auto-generated method stub
		this.first = first;
		this.second = second;
	}

	public Text getFirst()
	{
		return first;
	}

	public LongWritable getSecond()
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
		if (o instanceof TextLongPair)
		{
			TextLongPair tp = (TextLongPair) o;
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
	public int compareTo(TextLongPair o)
	{
		// TODO Auto-generated method stub
		int cmp = first.compareTo(o.first);
		if (cmp != 0)
			return cmp;
		return second.compareTo(o.second);
	}

}