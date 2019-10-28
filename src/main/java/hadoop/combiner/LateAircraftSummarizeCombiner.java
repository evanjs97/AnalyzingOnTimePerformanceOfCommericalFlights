package hadoop.combiner;

import hadoop.util.LateAircraftSummarizeWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LateAircraftSummarizeCombiner extends Reducer<Text, LateAircraftSummarizeWritable, Text, LateAircraftSummarizeWritable>{
	protected void reduce(Text key, Iterable<LateAircraftSummarizeWritable> values, Context context) throws IOException, InterruptedException {
		int lateDelaySum, numLateDelays, weatherDelaySum, numWeatherDelays, nasDelaySum, numNASDelays,
				securityDelaySum,  numSecurityDelays, carrierDelaySum, numCarrierDelays, flightCount;
		lateDelaySum = numLateDelays = weatherDelaySum = numWeatherDelays = nasDelaySum = numNASDelays
				= securityDelaySum =  numSecurityDelays = carrierDelaySum = numCarrierDelays = flightCount = 0;
		String name = "";
		for(LateAircraftSummarizeWritable lasw : values) {
			lateDelaySum += lasw.getLateDelaySum();
			numLateDelays += lasw.getNumLateDelays();
			weatherDelaySum += lasw.getWeatherDelaySum();
			numWeatherDelays += lasw.getNumWeatherDelays();
			nasDelaySum += lasw.getWeatherDelaySum();
			numNASDelays += lasw.getNumWeatherDelays();
			securityDelaySum += lasw.getSecurityDelaySum();
			numSecurityDelays += lasw.getNumSecurityDelays();
			carrierDelaySum += lasw.getCarrierDelaySum();
			numCarrierDelays += lasw.getNumCarrierDelays();
			flightCount += lasw.getFlightCount();
			if(name.isEmpty() && !lasw.getName().isEmpty()) name = lasw.getName();
		}

		LateAircraftSummarizeWritable lasw = new LateAircraftSummarizeWritable(lateDelaySum, numLateDelays, weatherDelaySum,
				numWeatherDelays, nasDelaySum, numNASDelays, securityDelaySum, numSecurityDelays, carrierDelaySum,
				numCarrierDelays, flightCount, name);
		context.write(new Text(key.toString()), lasw);
	}
}
