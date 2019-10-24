package hadoop.mapper;

import hadoop.util.AverageDelayWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EastWestMapper extends Mapper<LongWritable, Text, Text, AverageDelayWritable>{
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");

		if(Utils.isValidEntry(line[17])) {
			double temp = line[14].isEmpty() ? 0 : Double.parseDouble(line[14]);
			int wasDelayed = temp > 0 ? 1 : 0;
			AverageDelayWritable avg = new AverageDelayWritable(new IntWritable(wasDelayed), new DoubleWritable(1), new Text(""));
			context.write(new Text(line[17]), avg);
		}
		if(Utils.isValidEntry(line[16])) {
			double temp2 = line[15].isEmpty() ? 0 : Double.parseDouble(line[15]);
			int wasDelayed2 = temp2 > 0 ? 1 : 0;
			AverageDelayWritable avg2 = new AverageDelayWritable(new IntWritable(wasDelayed2), new DoubleWritable(1), new Text(""));
			context.write(new Text(line[16]), avg2);
		}
	}
}
