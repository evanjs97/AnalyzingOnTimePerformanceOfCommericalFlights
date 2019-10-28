package hadoop.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LateAircraftSummarizeWritable implements Writable{
	private IntWritable lateDelaySum;
	private IntWritable numLateDelays;

	private IntWritable weatherDelaySum;
	private IntWritable numWeatherDelays;

	private IntWritable nasDelaySum;
	private IntWritable numNASDelays;

	private IntWritable securityDelaySum;
	private IntWritable numSecurityDelays;

	private IntWritable carrierDelaySum;
	private IntWritable numCarrierDelays;

	private IntWritable flightCount;
	private Text name;

	public int getLateDelaySum() {
		return lateDelaySum.get();
	}

	public int getNumLateDelays() {
		return numLateDelays.get();
	}

	public int getWeatherDelaySum() {
		return weatherDelaySum.get();
	}

	public int getNumWeatherDelays() {
		return numWeatherDelays.get();
	}

	public int getNasDelaySum() {
		return nasDelaySum.get();
	}

	public int getNumNASDelays() {
		return numNASDelays.get();
	}

	public int getSecurityDelaySum() {
		return securityDelaySum.get();
	}

	public int getNumSecurityDelays() {
		return numSecurityDelays.get();
	}

	public int getCarrierDelaySum() {
		return carrierDelaySum.get();
	}

	public int getNumCarrierDelays() {
		return numCarrierDelays.get();
	}

	public int getFlightCount() {
		return flightCount.get();
	}

	public String getName() { return name.toString(); }

	public LateAircraftSummarizeWritable(int lateDelaySum, int numLateDelays, int weatherDelaySum, int numWeatherDelays,
										 int nasDelaySum, int numNASDelays, int securityDelaySum, int numSecurityDelays,
										 int carrierDelaySum, int numCarrierDelays, int flightCount) {
		this(lateDelaySum, numLateDelays, weatherDelaySum, numWeatherDelays, nasDelaySum, numNASDelays, securityDelaySum,
				numSecurityDelays, carrierDelaySum, numCarrierDelays, flightCount,"");
	}

	public LateAircraftSummarizeWritable(int lateDelaySum, int numLateDelays, int weatherDelaySum, int numWeatherDelays,
										 int nasDelaySum, int numNASDelays, int securityDelaySum, int numSecurityDelays,
										 int carrierDelaySum, int numCarrierDelays, int flightCount, String name) {
		this.lateDelaySum = new IntWritable(lateDelaySum);
		this.numLateDelays = new IntWritable(numLateDelays);
		this.weatherDelaySum = new IntWritable(weatherDelaySum);
		this.numWeatherDelays = new IntWritable(numWeatherDelays);
		this.nasDelaySum = new IntWritable(nasDelaySum);
		this.numNASDelays = new IntWritable(numNASDelays);
		this.securityDelaySum = new IntWritable(securityDelaySum);
		this.numSecurityDelays = new IntWritable(numSecurityDelays);
		this.carrierDelaySum = new IntWritable(carrierDelaySum);
		this.numCarrierDelays = new IntWritable(numCarrierDelays);
		this.flightCount = new IntWritable(flightCount);
		this.name = new Text(name);

	}

	public LateAircraftSummarizeWritable() {
		this(0,0,0,0,0,0,
				0,0,0,0,1);
	}


	@Override
	public void write(DataOutput out) throws IOException {
		this.lateDelaySum.write(out);
		this.numLateDelays.write(out);
		this.weatherDelaySum.write(out);
		this.numWeatherDelays.write(out);
		this.nasDelaySum.write(out);
		this.numNASDelays.write(out);
		this.securityDelaySum.write(out);
		this.numSecurityDelays.write(out);
		this.carrierDelaySum.write(out);
		this.numCarrierDelays.write(out);
		this.flightCount.write(out);
		this.name.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.lateDelaySum.readFields(in);
		this.numLateDelays.readFields(in);
		this.weatherDelaySum.readFields(in);
		this.numWeatherDelays.readFields(in);
		this.nasDelaySum.readFields(in);
		this.numNASDelays.readFields(in);
		this.securityDelaySum.readFields(in);
		this.numSecurityDelays.readFields(in);
		this.carrierDelaySum.readFields(in);
		this.numCarrierDelays.readFields(in);
		this.flightCount.readFields(in);
		this.name.readFields(in);
	}
}
