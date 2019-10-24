package hadoop.combiner;

import hadoop.util.AverageDelayWritable;
import hadoop.util.DoubleTextWritable;
import hadoop.util.IntTextWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherDelayCombiner extends Reducer<Text, AverageDelayWritable, Text, AverageDelayWritable>{
	public void reduce(Text key, Iterable<AverageDelayWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		double average = 0;
		String text = "";
		for(AverageDelayWritable value : values) {
			count += value.getCount().get();
			average += value.getValue().get();
			if(text.isEmpty() && !value.getType().toString().isEmpty()) text = value.getType().toString();
		}
		AverageDelayWritable avg = new AverageDelayWritable(new IntWritable(count), new DoubleWritable(average), new Text(text));
		context.write(new Text(key.toString()), avg);
	}
}
