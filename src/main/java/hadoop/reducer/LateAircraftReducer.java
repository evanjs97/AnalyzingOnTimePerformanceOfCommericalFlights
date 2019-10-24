package hadoop.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LateAircraftReducer extends Reducer<Text, IntWritable, Text, NullWritable>{
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) {

	}
}
