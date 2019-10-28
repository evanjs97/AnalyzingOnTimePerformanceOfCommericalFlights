package hadoop.mapper;

import hadoop.util.LateAircraftWritable;
import hadoop.util.Utils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LateAircraftMapper extends Mapper<LongWritable, Text, Text, LateAircraftWritable> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");

		if(line.length > 28 && Utils.isValidEntry(line[10]) && Utils.isValidEntry(line[28])) {
			String carrier_id = line[10];
			int lateDelay = (int) Double.parseDouble(line[28]);
			int secDelay = (int) Double.parseDouble(line[27]);
			int nasDelay = (int) Double.parseDouble(line[26]);
			int weatherDelay = (int) Double.parseDouble(line[25]);
			int carrierDelay = (int) Double.parseDouble(line[24]);

			int year = Integer.parseInt(line[0]);
			int month = Integer.parseInt(line[1]);
			int dayOfMonth = Integer.parseInt(line[2]);
			context.write(new Text(carrier_id), new LateAircraftWritable(lateDelay, secDelay, nasDelay, weatherDelay,
					carrierDelay, year, month, dayOfMonth, line[4], line[8], line[16]));
		}
	}
}
