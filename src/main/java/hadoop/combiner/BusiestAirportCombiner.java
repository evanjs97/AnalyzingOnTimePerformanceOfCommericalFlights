package hadoop.combiner;

import hadoop.util.IntTextWritable;
import hadoop.util.WordCount;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class BusiestAirportCombiner extends Reducer<Text, IntTextWritable, Text, IntTextWritable>{
		public void reduce(Text key, Iterable<IntTextWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			String text = "";
			for(IntTextWritable value : values) {
				count += value.getCount().get();
				if(text.isEmpty() && !value.getName().toString().isEmpty()) text = value.getName().toString();
			}
			if(!text.isEmpty()) context.write(new Text(key.toString()), new IntTextWritable(new IntWritable(count), new Text(text)));
		}
}
