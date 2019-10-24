package hadoop.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LateAircraftWritable implements Writable{
	private IntWritable delay;
	private Text time;

	public IntWritable getDelay() {
		return delay;
	}

	public Text getTime() {
		return time;
	}

	public LateAircraftWritable(int delay, String time) {
		this.delay = new IntWritable(delay);
		this.time = new Text(time);
	}

	public LateAircraftWritable() {
		delay = new IntWritable(0);
		time = new Text("");
	}

	@Override
	public String toString() {
		return time.toString().substring(0,2) + ":" + time.toString().substring(2) + "-" + delay.get();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		delay.write(out);
		time.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		delay.readFields(in);
		time.readFields(in);
	}
}
