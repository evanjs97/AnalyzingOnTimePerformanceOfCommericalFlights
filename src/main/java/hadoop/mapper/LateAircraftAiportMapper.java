package hadoop.mapper;

import hadoop.util.AverageDelayWritable;
import hadoop.util.LateAircraftSummarizeWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LateAircraftAiportMapper extends Mapper<LongWritable, Text, Text, AverageDelayWritable> {
	protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");

		if(!Utils.isValidEntry(line[0])) return;
		LateAircraftSummarizeWritable lasw = new LateAircraftSummarizeWritable(0,0,
				0,0,0,0,0,
				0,0,0,0, line[1].replaceAll("\"", ""));
		context.write(new Text("A"+line[0].replaceAll("\"", "")), lasw);
	}
}