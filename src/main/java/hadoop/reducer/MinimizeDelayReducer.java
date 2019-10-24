package hadoop.reducer;

import hadoop.util.AverageDelayWritable;
import hadoop.util.DoubleAveragePair;
import hadoop.util.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class MinimizeDelayReducer extends Reducer<Text, AverageDelayWritable, Text, Text>{


	protected void reduce(Text key, Iterable<AverageDelayWritable> values, Context context) throws IOException, InterruptedException {
//		for(AverageDelayWritable val : values) {
//			context.write(new Text(key), new Text(String.format("%s: Average: %f", val.getType().toString(), val.getAverage())));
//		}

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



		List<Map.Entry<String, AverageDelayWritable>> list = new ArrayList<>(averages.entrySet());
		list.sort(Comparator.comparingDouble(avg -> avg.getValue().getAverage()));

//		for(Map.Entry<String, AverageDelayWritable> entry : list) {
//			context.write(new Text(key+"\t"+format(entry.getKey(), key.toString())), new Text(""+entry.getValue().getAverage()));
//		}
		String w = String.format("%s with average delay of %.2f", format(list.get(0).getKey(), key.toString()), list.get(0).getValue().getAverage());
		String b = String.format("%s with average delay of %.2f", format(list.get(list.size()-1).getKey(), key.toString()), list.get(list.size()-1).getValue().getAverage());
		Text best = new Text(b);
		Text worst = new Text(w);
		Text worstType = new Text("Best Departure " + key + " to Minimize Delays: ");
		Text bestType =  new Text("Worst Departure " + key +" to Minimize Delays: ");

		context.write(bestType, best);
		context.write(worstType, worst);
	}

	protected String format(String key, String type) {

		switch (type) {
			case "Time":
				return key.substring(0,2)+":"+key.substring(2);
			case "Day":
				return Utils.getDay(Integer.parseInt(key)-1);
			case "Month":
				return Utils.getMonth(Integer.parseInt(key)-1);
		}
		return "";
	}
}
