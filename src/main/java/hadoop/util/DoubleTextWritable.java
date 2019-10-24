package hadoop.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleTextWritable implements Writable{
	private DoubleWritable count;
	private Text name;

	public DoubleWritable getCount() {
		return count;
	}

	public Text getName() {
		return name;
	}

	public DoubleTextWritable(DoubleWritable count, Text name) {
		this.count = count;
		this.name = name;
	}

	public DoubleTextWritable() {
		this.count = new DoubleWritable(0);
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
