package hadoop.reducer;

import hadoop.util.AverageDelayWritable;
import hadoop.util.IntDoubleWordCount;
import hadoop.util.Utils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class CarrierDelayReducer extends Reducer<Text, AverageDelayWritable, Text, Text>{

	TreeSet<IntDoubleWordCount> tree = new TreeSet<>();
	HashMap<String, String> carrierMapping = new HashMap<>();
	protected void reduce(Text key, Iterable<AverageDelayWritable> values, Context context) {

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

		if(!name.isEmpty()) carrierMapping.put(key.toString(), name);
		if(count > 0) tree.add(new IntDoubleWordCount(key.toString(), average, count));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		List<IntDoubleWordCount> sorting = new ArrayList<>(tree);
		sorting.sort(Comparator.comparingDouble(IntDoubleWordCount::getCount));
		context.write(new Text("Sorted by number of delays"), new Text(""));
		for(int i = 0; i < 10; i++) {
			IntDoubleWordCount avg = sorting.get(sorting.size()-1-i);
			String name = carrierMapping.get(avg.getWord());
			String delay = String.format("Flights Delayed: %d Total Delay: %.0f" ,avg.getCount(), avg.getSum());
			context.write(new Text(Utils.reformatString(avg.getWord()+"\t"+name, 40)), new Text(delay));
		}
		sorting.sort(Comparator.comparingDouble(IntDoubleWordCount::getSum));
		context.write(new Text("\n"), new Text("\n"));
		context.write(new Text("Sorted by total delay"), new Text(""));
		for(int i = 0; i < 10; i++) {
			IntDoubleWordCount avg = sorting.get(sorting.size()-1-i);
			String name = carrierMapping.get(avg.getWord());
			String delay = String.format("Flights Delayed: %d Total Delay: %.0f" ,avg.getCount(), avg.getSum());
			context.write(new Text(Utils.reformatString(avg.getWord()+"\t"+name, 40)), new Text(delay));
		}
		sorting.sort(Comparator.comparingDouble(entry -> entry.getSum() / entry.getCount()));
		context.write(new Text("\n"), new Text("\n"));
		context.write(new Text("Sorted by average delay length"), new Text(""));
		for(int i = 0; i < 10; i++) {
			IntDoubleWordCount avg = sorting.get(sorting.size()-1-i);
			String name = carrierMapping.get(avg.getWord());
			String delay = String.format("Flights Delayed: %d Total Delay: %.0f" ,avg.getCount(), avg.getSum());
			context.write(new Text(Utils.reformatString(avg.getWord()+"\t"+name, 40)), new Text(delay));
		}
	}
}
