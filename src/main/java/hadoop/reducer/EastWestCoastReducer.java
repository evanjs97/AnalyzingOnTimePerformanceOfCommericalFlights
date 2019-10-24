package hadoop.reducer;

import hadoop.util.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EastWestCoastReducer extends Reducer<Text, AverageDelayWritable, Text, NullWritable> {

	private int eastDelayCount;
	private int westDelayCount;
	private double eastDelay;
	private double westDelay;
	private int otherDelayCount;
	private double otherDelay;
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
		if(name.isEmpty()) return;
		if(name.equals("EAST")) {
			eastDelayCount += count;
			eastDelay += average;
		}else if(name.equals("WEST")) {
			westDelayCount += count;
			westDelay += average;
		}else {
			otherDelay += average;
			otherDelayCount += count;
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		String west = String.format("West Coast Delay Flight Count: %d\nWest Coast Flight Count: %.0f\n" +
				"West Coast Delayed Flight Average Duration: %.2f%%", westDelayCount, westDelay,  (westDelayCount / westDelay) * 100);
		String east = String.format("East Coast Delay Flight Count: %d\nEast Coast Flight Count: %.0f\n" +
				"East Coast Delayed Flight Average Duration: %.2f%%", eastDelayCount, eastDelay, (eastDelayCount / eastDelay) * 100);
		String other = String.format("Other Coast Delay Flight Count: %d\nOther Coast Flight Count: %.0f\n" +
				"Other Coast Delayed Flight Average Duration: %.2f%%", otherDelayCount, otherDelay, (otherDelayCount / otherDelay) * 100);
		context.write(new Text(west), NullWritable.get());
		context.write(new Text(east), NullWritable.get());
		context.write(new Text(other), NullWritable.get());
	}
}
