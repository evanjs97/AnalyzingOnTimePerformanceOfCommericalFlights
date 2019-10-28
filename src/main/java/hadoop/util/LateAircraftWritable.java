package hadoop.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LateAircraftWritable implements Writable{
	private IntWritable lateAircraftDelay;
	private IntWritable securityDelay;
	private IntWritable NASDelay;
	private IntWritable weatherDelay;
	private IntWritable carrierDelay;
	private IntWritable year;
	private IntWritable month;
	private IntWritable dayOfMonth;
	private Text timeOfDay;
	private Text carrierCode;
	private Text airportCode;



	public IntWritable getLateAircraftDelay() {
		return lateAircraftDelay;
	}

	public IntWritable getSecurityDelay() {
		return securityDelay;
	}

	public IntWritable getNASDelay() {
		return NASDelay;
	}

	public IntWritable getWeatherDelay() {
		return weatherDelay;
	}

	public IntWritable getCarrierDelay() {
		return carrierDelay;
	}

	public IntWritable getYear() {
		return year;
	}

	public IntWritable getMonth() {
		return month;
	}

	public IntWritable getDayOfMonth() {
		return dayOfMonth;
	}

	public Text getTimeOfDay() {
		return timeOfDay;
	}

	public Text getCarrierCode() {
		return carrierCode;
	}

	public Text getAirportCode() { return airportCode; }

	public Text getTime() {
		return timeOfDay;
	}

	public LateAircraftWritable(int lateDelay, int securityDelay, int nasDelay, int weatherDelay, int carrierDelay,
								int year, int month, int day, String time, String carrierCode, String airportCode) {
		this.lateAircraftDelay = new IntWritable(lateDelay);
		this.securityDelay = new IntWritable(securityDelay);
		this.NASDelay = new IntWritable(nasDelay);
		this.weatherDelay = new IntWritable(weatherDelay);
		this.carrierDelay = new IntWritable(carrierDelay);
		this.year = new IntWritable(year);
		this.month = new IntWritable(month);
		this.dayOfMonth = new IntWritable(day);
		this.timeOfDay = new Text(time);
		this.carrierCode = new Text(carrierCode);
		this.airportCode = new Text(airportCode);
	}

	public LateAircraftWritable() {
		lateAircraftDelay = new IntWritable(0);
		securityDelay = new IntWritable(0);
		NASDelay = new IntWritable(0);
		weatherDelay = new IntWritable(0);
		carrierDelay = new IntWritable(0);
		year = new IntWritable(0);
		month = new IntWritable(0);
		dayOfMonth = new IntWritable(0);
		timeOfDay = new Text("");
		carrierCode = new Text("");
		airportCode = new Text("");
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(timeOfDay.toString().substring(0,2));
		builder.append(':');
		builder.append(timeOfDay.toString().substring(2));
		builder.append(' ');
		builder.append(dayOfMonth.get());
		builder.append(' ');
		builder.append(Utils.getMonth(month.get()));
		builder.append(' ');
		builder.append(Utils.formatDayOfMonth(dayOfMonth.get()));
		builder.append(" ");
		builder.append(year);
		builder.append(' ');
		builder.append(lateAircraftDelay.get());
		builder.append(' ');
		builder.append(securityDelay.get());
		builder.append(' ');
		builder.append(NASDelay.get());
		builder.append(' ');
		builder.append(weatherDelay.get());
		builder.append(' ');
		builder.append(carrierDelay.get());
		builder.append(' ');
		builder.append(carrierCode.toString());
		builder.append(' ');
		builder.append(airportCode.toString());
		return builder.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		lateAircraftDelay.write(out);
		securityDelay.write(out);
		NASDelay.write(out);
		weatherDelay.write(out);
		carrierDelay.write(out);
		year.write(out);
		month.write(out);
		dayOfMonth.write(out);
		timeOfDay.write(out);
		carrierCode.write(out);
		airportCode.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		lateAircraftDelay.readFields(in);
		securityDelay.readFields(in);
		NASDelay.readFields(in);
		weatherDelay.readFields(in);
		carrierDelay.readFields(in);
		year.readFields(in);
		month.readFields(in);
		dayOfMonth.readFields(in);
		timeOfDay.readFields(in);
		carrierCode.readFields(in);
		airportCode.readFields(in);
	}
}
