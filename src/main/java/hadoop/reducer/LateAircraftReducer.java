package hadoop.reducer;

import hadoop.util.DelaySorter;
import hadoop.util.LateAircraftWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LateAircraftReducer extends Reducer<Text, LateAircraftWritable, Text, NullWritable>{



	protected void reduce(Text key, Iterable<LateAircraftWritable> values, Context context) throws IOException, InterruptedException {
		List<DelaySorter> times = new ArrayList<>();
		for(LateAircraftWritable law : values) {
			times.add(new DelaySorter(law));
		}

		Collections.sort(times);
//		times.sort(Comparator.comparing(DelayPair::getTime, (e1, e2) -> {
//			int h1 = Integer.parseInt(e1.substring(0,2));
//			int h2 = Integer.parseInt(e2.substring(0,2));
//			int m1 = Integer.parseInt(e1.substring(2));
//			int m2 = Integer.parseInt(e2.substring(2));
//			int hourCompare = Integer.compare(h1, h2);
//			return hourCompare == 0 ? Integer.compare(m1, m2) : hourCompare;
//		}));

		StringBuilder format = new StringBuilder();
		for(DelaySorter delay : times) {
			format.append(delay.toString());
			format.append('\t');
		}
		context.write(new Text(format.toString()), NullWritable.get());

	}
}
