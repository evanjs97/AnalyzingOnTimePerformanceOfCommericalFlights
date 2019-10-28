package hadoop.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LateAircraftCarrier implements Writable{

	private IntWritable carrierDelaySum;
	private IntWritable numCarrierDelays;

	public LateAircraftCarrier(int carrierDelaySum, int numCarrierDelays) {
		this.carrierDelaySum = new IntWritable(carrierDelaySum);
		this.numCarrierDelays = new IntWritable(numCarrierDelays);
	}

	public LateAircraftCarrier() {
		this.carrierDelaySum = new IntWritable(0);
		this.numCarrierDelays = new IntWritable(0);
	}


	@Override
	public void write(DataOutput out) throws IOException {
		this.carrierDelaySum.write(out);
		this.numCarrierDelays.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.carrierDelaySum.readFields(in);
		this.numCarrierDelays.readFields(in);
	}
}
