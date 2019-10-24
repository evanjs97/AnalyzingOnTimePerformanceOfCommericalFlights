package hadoop.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntTextWritable implements Writable{
	private IntWritable count;
	private Text name;

	public IntWritable getCount() {
		return count;
	}

	public Text getName() {
		return name;
	}

	public IntTextWritable(IntWritable count, Text name) {
		this.count = count;
		this.name = name;
	}

	public IntTextWritable() {
		this.count = new IntWritable(0);
		this.name = new Text("");
	}

	@Override
	public void write(DataOutput out) throws IOException {
		name.write(out);
		count.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		name.readFields(in);
		count.readFields(in);
	}
}
