package hadoop.combiner;

import hadoop.util.AverageDelayWritable;
import hadoop.util.IntDoubleWordCount;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarrierDelayCombiner extends Reducer<Text, AverageDelayWritable, Text, AverageDelayWritable> {
	protected void reduce(Text key, Iterable<AverageDelayWritable> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		double average = 0;
		String name = "";

		for(AverageDelayWritable avg : values) {
			count += avg.getCount().get();
			average += avg.getValue().get();
			if(name.isEmpty() && !avg.getType().toString().isEmpty()) {
				name = avg.getType().toString();
			}
		}

		AverageDelayWritable avg = new AverageDelayWritable(new IntWritable(count), new DoubleWritable(average), new Text(name));
		context.write(new Text(key.toString()), avg);
	}
}
