package hadoop.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AverageDelayWritable implements Writable{
	private IntWritable count;
	private DoubleWritable value;
	private Text type;

	public IntWritable getCount() {
		return count;
	}

	public DoubleWritable getValue() {
		return value;
	}

	public Text getType() {
		return type;
	}

	public double getAverage() {
		return value.get() / count.get();
	}

	public void addToAverage(double add) {
		value.set(value.get()+add);
	}

	public void addToCount(int add) {
		count.set(count.get()+add);
	}

	public AverageDelayWritable() {
		this(new IntWritable(0), new DoubleWritable(0), new Text(""));
	}

	public AverageDelayWritable(IntWritable count, DoubleWritable value, Text type) {
		this.count = count;
		this.value = value;
		this.type = type;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		type.write(out);
		value.write(out);
		count.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		type.readFields(in);
		value.readFields(in);
		count.readFields(in);
	}
}
