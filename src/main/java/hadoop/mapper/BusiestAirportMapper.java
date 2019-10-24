package hadoop.mapper;

import hadoop.util.IntTextWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BusiestAirportMapper extends Mapper<LongWritable, Text, Text, IntTextWritable> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");

		if(Utils.isValidEntry(line[17])) {
			IntTextWritable pair = new IntTextWritable(new IntWritable(1), new Text(line[0]));
			context.write(new Text(line[17]), pair);
			//if(Utils.isValidEntry(line[0])) context.write(new Text("YEAR"+line[17]), new IntTextWritable(new IntWritable(1), new Text(line[0]));
		}
		if(Utils.isValidEntry(line[16])) {
			IntTextWritable pair = new IntTextWritable(new IntWritable(1), new Text(line[0]));
			context.write(new Text(line[16]), pair);
			//if(Utils.isValidEntry(line[0])) context.write(new Text("YEAR"+line[16]), new IntTextWritable(new IntWritable(1), new Text(line[0]));
		}
			
	}
}
