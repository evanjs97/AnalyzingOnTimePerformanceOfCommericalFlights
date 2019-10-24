package hadoop.reducer;

import hadoop.util.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeSet;

public class WeatherDelayReducer extends Reducer<Text, AverageDelayWritable, Text, Text> {
	protected TreeSet<IntDoubleWordCount> tree = new TreeSet<>();
	protected HashMap<String, String> codeMapping = new HashMap<>();

	protected void reduce(Text key, Iterable<AverageDelayWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		String text = "";
		double average = 0;
		for(AverageDelayWritable val : values) {
			sum += val.getCount().get();
			average += val.getValue().get();
			if(text.isEmpty() && !val.getType().toString().isEmpty()) {
				text = val.getType().toString();
			}
		}
		if(sum > 0) {
			tree.add(new IntDoubleWordCount(key.toString(), average, sum));
			codeMapping.put(key.toString(), text);
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		int counter = 0;
		while(counter < 10 && !tree.isEmpty()) {
			IntDoubleWordCount current = tree.pollLast();
//			DoubleWritable delay  = new DoubleWritable(Double.parseDouble(String.format("%.0f",current.getCount())));
			String delayString = String.format("Flights Delayed: %d\t  Minutes Spent in Weather Delay: %.2f", current.getCount(), current.getSum());
			context.write(new Text(Utils.reformatString(current.getWord()+"\t"+codeMapping.get(current.getWord()), 45)), new Text(delayString));
			counter++;
		}
	}
}
