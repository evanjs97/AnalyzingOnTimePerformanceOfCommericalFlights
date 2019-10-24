package hadoop.mapper;

import hadoop.util.IntTextWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportCodeJoinMapper extends Mapper<LongWritable, Text, Text, IntTextWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");

		if(Utils.isValidEntry(line[0])) {
			context.write(new Text(line[0].replaceAll("\"", "")), new IntTextWritable(new IntWritable(0), new Text(line[1].replaceAll("\"", ""))));
			//context.write(new Text("YEAR"+line[0].replaceAll("\"", "")), new IntTextWritable(new IntWritable(0), new Text(line[1].replaceAll("\"", ""))));
		}
	}
}
