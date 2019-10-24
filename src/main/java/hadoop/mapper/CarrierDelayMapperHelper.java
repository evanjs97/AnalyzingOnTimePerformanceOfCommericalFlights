package hadoop.mapper;

import hadoop.util.AverageDelayWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.rmi.CORBA.Util;
import java.io.IOException;

public class CarrierDelayMapperHelper extends Mapper<LongWritable, Text, Text, AverageDelayWritable> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");

		if(!Utils.isValidEntry(line[0])) return;

		AverageDelayWritable avg = new AverageDelayWritable(new IntWritable(0), new DoubleWritable(0), new Text(line[1].replaceAll("\"", "")));
		context.write(new Text(line[0].replaceAll("\"", "")), avg);
	}
}
