package hadoop.reducer;

import hadoop.util.LateAircraftWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class LateAircraftReducer extends Reducer<Text, LateAircraftWritable, Text, NullWritable>{
	protected void reduce(Text key, Iterable<LateAircraftWritable> values, Context context) throws IOException, InterruptedException {
		List<LateAircraftWritable> times = new ArrayList<>();
		for(LateAircraftWritable law : values) {
			times.add(law);
		}

		times.sort(Comparator.comparing(LateAircraftWritable::getTime, (e1, e2) -> {
			String es1 = e1.toString();
			String es2 = e2.toString();
			int h1 = Integer.parseInt(es1.substring(0,2));
			int h2 = Integer.parseInt(es2.substring(0,2));
			int m1 = Integer.parseInt(es1.substring(2));
			int m2 = Integer.parseInt(es2.substring(2));
			int hourCompare = Integer.compare(h1, h2);
			return hourCompare == 0 ? Integer.compare(m1, m2) : hourCompare;
		}));

		StringBuilder format = new StringBuilder();
		format.append(key);
		format.append(":\t");
		for(LateAircraftWritable law : times) {
			format.append(law.toString());
			format.append(" -- >");
		}
		context.write(new Text(format.toString()), NullWritable.get());
	}
}
