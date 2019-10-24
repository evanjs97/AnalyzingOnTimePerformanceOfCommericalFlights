package hadoop.mapper;

import hadoop.util.AverageDelayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class EastWestCoastMapperHelper extends Mapper<LongWritable, Text, Text, AverageDelayWritable> {
	private static final String[] eastCoastStates = new String[]{"ME", "NH", "NY", "MA", "RI", "CT",
			"NJ", "PA", "DE", "MD", "DC", "SC", "NC", "GA", "FL", "VA"};
	private static final String[] westCoastStates = new String[]{"WA", "OR", "CA", "AK"};
	private static final HashSet<String> eastCoast = new HashSet<>(Arrays.asList(eastCoastStates));
	private static final HashSet<String> westCoast = new HashSet<>(Arrays.asList(westCoastStates));

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");

		String coast = "NEITHER";
		if(eastCoast.contains(line[3].replaceAll("\"", ""))) coast = "EAST";
		else if(westCoast.contains(line[3].replaceAll("\"", ""))) coast = "WEST";


		AverageDelayWritable avg = new AverageDelayWritable(new IntWritable(0), new DoubleWritable(0), new Text(coast));
		context.write(new Text(line[0].replaceAll("\"", "")), avg);
	}
}
