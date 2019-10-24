package hadoop.mapper;

import hadoop.util.AverageDelayWritable;
import hadoop.util.IntTextWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

public class WeatherDelayMapperHelper extends Mapper<LongWritable, Text, Text, AverageDelayWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");

		if(Utils.isValidEntry(line[0])) {
			AverageDelayWritable avg = new AverageDelayWritable(new IntWritable(0), new DoubleWritable(0), new Text(line[2].replaceAll("\"", "")));
			context.write(new Text(line[0].replaceAll("\"", "")), avg);
			//context.write(new Text("YEAR"+line[0].replaceAll("\"", "")), new IntTextWritable(new IntWritable(0), new Text(line[1].replaceAll("\"", ""))));
		}
	}
}
