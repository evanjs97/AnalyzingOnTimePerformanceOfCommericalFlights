package hadoop.mapper;

import hadoop.util.AverageDelayWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarrierDelayMapper extends Mapper<LongWritable, Text, Text, AverageDelayWritable> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");
		if(line.length < 25 || !Utils.isValidEntry(line[8])) return;

		String carrierCode = line[8];
		double carrierDelay = line[24].isEmpty() ? 0 : Double.parseDouble(line[24]);
		int delay = carrierDelay > 0 ? 1 : 0;

		AverageDelayWritable avg = new AverageDelayWritable(new IntWritable(delay), new DoubleWritable(carrierDelay), new Text(""));

		context.write(new Text(carrierCode), avg);
	}
}
