package hadoop.mapper;

import hadoop.util.AverageDelayWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MinimizeDelayMapper extends Mapper<LongWritable, Text, Text, AverageDelayWritable> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");

//		if(!Utils.isValidEntry(line[15]) && !Utils.isValidEntry(line[14])) {
//			return;
//		}

		double totalDelay = 0;
		if(Utils.isValidEntry(line[14])) {
			totalDelay += Double.parseDouble(line[14]);
		}
		if(Utils.isValidEntry(line[15])) {
			totalDelay+= Double.parseDouble(line[15]);
		}

		DoubleWritable delay = new DoubleWritable(totalDelay);

		if(Utils.isValidEntry(line[5])) {
//			Writable[] arr = new Writable[]{delay, new IntWritable(1), new Text(line[5])};
			int year = Integer.parseInt(line[0]);
			int month = Integer.parseInt(line[1]);
			int day = Integer.parseInt(line[2]);
			String roundedTime = Utils.roundTime(year, month, day, line[5], 15);
			context.write(new Text("Time"), new AverageDelayWritable(new IntWritable(1), delay, new Text(roundedTime)));
		}

		if(Utils.isValidEntry(line[3])) {//day of week
//			Writable[] arr = new Writable[]{delay, new IntWritable(1), new Text(line[5])};
			context.write(new Text("Day"), new AverageDelayWritable(new IntWritable(1), delay, new Text(line[3])));
		}
		if(Utils.isValidEntry(line[1])) {
//			Writable[] arr = new Writable[]{delay, new IntWritable(1), new Text(line[5])};
			context.write(new Text("Month"), new AverageDelayWritable(new IntWritable(1), delay, new Text(line[1])));
		}

 	}
}
