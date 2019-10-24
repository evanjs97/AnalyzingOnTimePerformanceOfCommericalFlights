package hadoop.mapper;

import hadoop.util.AverageDelayWritable;
import hadoop.util.DoubleTextWritable;
import hadoop.util.IntTextWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherDelayMapper extends Mapper<LongWritable, Text, Text, AverageDelayWritable> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");

		if(line.length < 26 || !Utils.isValidEntry(line[25])) return;

		double weatherDelay = Double.parseDouble(line[25]);

//		int arrivalDelay = Utils.isValidEntry(line[14]) ? Integer.parseInt(line[14]) : 0;
		int wasDelayed = weatherDelay > 0 ? 1 : 0;
		AverageDelayWritable avg = new AverageDelayWritable(new IntWritable(wasDelayed), new DoubleWritable(weatherDelay), new Text(""));
		context.write(new Text(line[16]), avg);
//		if(arrivalDelay >= weatherDelay && departureDelay < weatherDelay) {
//			context.write(new Text(line[17]), new DoubleTextWritable(new DoubleWritable(weatherDelay), new Text("")));
//		}else {
//			context.write(new Text(line[16]), new DoubleTextWritable(new DoubleWritable(weatherDelay), new Text("")));
//		}
	}
}
