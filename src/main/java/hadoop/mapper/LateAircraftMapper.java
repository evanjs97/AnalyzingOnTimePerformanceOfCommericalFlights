package hadoop.mapper;

import hadoop.util.LateAircraftWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LateAircraftMapper extends Mapper<LongWritable, Text, Text, LateAircraftWritable> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");

		if(Utils.isValidEntry(line[10]) && Utils.isValidEntry(line[28])) {
			String builder = "Y" +
					line[0] +
					'M' +
					line[1] +
					'D' +
					line[2] +
					'N' +
					line[10];
			int minutesDelayed = (int) Double.parseDouble(line[28]);
			context.write(new Text(builder), new LateAircraftWritable(minutesDelayed, line[4]));
		}
	}
}
