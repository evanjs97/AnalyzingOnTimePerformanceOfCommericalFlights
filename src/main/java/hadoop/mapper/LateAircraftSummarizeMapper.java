package hadoop.mapper;

import hadoop.util.LateAircraftSummarizeWritable;
import hadoop.util.LateAircraftCarrier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LateAircraftSummarizeMapper extends Mapper<LongWritable, Text, Text, LateAircraftSummarizeWritable> {

	//output {airport_code, (
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		String[] prev = line[0].split(",");
		for(int i = 0; i < line.length; i++) {
			String[] entry = line[i].split(",");
			if(entry.length == 0) continue;
			//[time(0),day(1),month(2),year(3),lateDelay(4),secDelay(5),nasDelay(6),weatherDelay(7),carrierDelay(8),
			// carrierCode(9), airportCode(10)]
			int lateDelay = Integer.parseInt(entry[4]);
			int carrierDelay = Integer.parseInt(prev[8]);
			int secDelay = Integer.parseInt(prev[5]);
			int nasDelay = Integer.parseInt(prev[6]);
			int weatherDelay = Integer.parseInt(prev[7]);
			LateAircraftSummarizeWritable temp = new LateAircraftSummarizeWritable(0, 0,
                                                0, 0, 0, 0, 0,
                                                0, 0, 0, 1);
			if (lateDelay > 0 && (carrierDelay > 0 || secDelay > 0 || nasDelay > 0 || weatherDelay > 0)) {

				LateAircraftSummarizeWritable lasw;
				if (carrierDelay > secDelay && carrierDelay > nasDelay && carrierDelay > weatherDelay) {
					lasw = new LateAircraftSummarizeWritable(0, 0, 0,
							0, 0, 0, 0,
							0, carrierDelay, 1, 1);
					context.write(new Text("C" + prev[9]), lasw);
					context.write(new Text("A" + prev[10]), temp);
				} else {
					if (nasDelay >= secDelay && nasDelay >= weatherDelay) {
						lasw = new LateAircraftSummarizeWritable(lateDelay, 1, 0,
								0, nasDelay, 1, 0, 0,
								0, 0, 1);
					} else if (weatherDelay >= secDelay && weatherDelay >= nasDelay) {
						lasw = new LateAircraftSummarizeWritable(lateDelay, 1, weatherDelay,
								1, 0, 0, 0,
								0, 0, 0, 1);
					} else {
						lasw = new LateAircraftSummarizeWritable(lateDelay, 1, 0,
								0, 0, 0, secDelay, 1,
								0, 0, 1);
					}
					context.write(new Text("A" + prev[10]), lasw);
					context.write(new Text("C" + prev[9]), temp);
				}
			} else {
				//LateAircraftSummarizeWritable lasw = new LateAircraftSummarizeWritable(0, 0,
						//0, 0, 0, 0, 0,
						//0, 0, 0, 1);
				context.write(new Text("C" + prev[9]), temp);
				context.write(new Text("A" + prev[10]), temp);

			}
			if(i == line.length-1) {
				context.write(new Text("C" + entry[9]), temp);
				context.write(new Text("A" + entry[10]), temp);
			}
			prev = entry;
		}
	}
}
