package hadoop.combiner;

import hadoop.util.AverageDelayWritable;
import hadoop.util.DoubleAveragePair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MinimizeDelayCombiner extends Reducer<Text, AverageDelayWritable, Text, AverageDelayWritable>{

	protected void reduce(Text key, Iterable<AverageDelayWritable> values, Context context) throws IOException, InterruptedException {
		HashMap<String, AverageDelayWritable> averages = new HashMap<>();
		for(AverageDelayWritable avgd : values) {

			AverageDelayWritable value = averages.remove(avgd.getType().toString());
			if(value == null) {
				value = new AverageDelayWritable(new IntWritable(avgd.getCount().get()), new DoubleWritable(avgd.getValue().get()), new Text(avgd.getType()));
			}
			else {
				value.addToCount(avgd.getCount().get());
				value.addToAverage(avgd.getValue().get());
			}

			averages.put(avgd.getType().toString(), value);
		}

		for(Map.Entry<String, AverageDelayWritable> entry : averages.entrySet()) {
			context.write(new Text(key.toString()), new AverageDelayWritable(entry.getValue().getCount(), entry.getValue().getValue(), entry.getValue().getType()));
		}
	}
}
