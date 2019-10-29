package hadoop.util;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class LateAircraftSummarizeReducer extends Reducer<Text, LateAircraftSummarizeWritable, Text, NullWritable> {
	private List<DelaySummarizeSorter> airports = new ArrayList<>();
	private List<DelaySummarizeSorter> carriers = new ArrayList<>();
	private List<DelaySummarizeSorter> busyAirports = new ArrayList<>();
	private List<DelaySummarizeSorter> busiestAirports = new ArrayList<>();
	protected void reduce(Text key, Iterable<LateAircraftSummarizeWritable> values, Context context) throws IOException, InterruptedException {
		int lateDelaySum, numLateDelays, weatherDelaySum, numWeatherDelays, nasDelaySum, numNASDelays,
				securityDelaySum, numSecurityDelays, carrierDelaySum, numCarrierDelays, flightCount;
		lateDelaySum = numLateDelays = weatherDelaySum = numWeatherDelays = nasDelaySum = numNASDelays
				= securityDelaySum = numSecurityDelays = carrierDelaySum = numCarrierDelays = flightCount = 0;
		String name = "";
		for (LateAircraftSummarizeWritable lasw : values) {
			lateDelaySum += lasw.getLateDelaySum();
			numLateDelays += lasw.getNumLateDelays();
			weatherDelaySum += lasw.getWeatherDelaySum();
			numWeatherDelays += lasw.getNumWeatherDelays();
			nasDelaySum += lasw.getNASDelaySum();
			numNASDelays += lasw.getNumNASDelays();
			securityDelaySum += lasw.getSecurityDelaySum();
			numSecurityDelays += lasw.getNumSecurityDelays();
			carrierDelaySum += lasw.getCarrierDelaySum();
			numCarrierDelays += lasw.getNumCarrierDelays();
			flightCount += lasw.getFlightCount();
			if (name.isEmpty() && !lasw.getName().isEmpty()) name = lasw.getName();
		}
		if(flightCount < 1000) return;
		if(key.charAt(0) == 'A' && lateDelaySum == 0) return;
		if(key.charAt(0) == 'C' && carrierDelaySum == 0) return;
		DelaySummarizeSorter ds = new DelaySummarizeSorter(lateDelaySum, numLateDelays, weatherDelaySum, numWeatherDelays,
				nasDelaySum, numNASDelays, securityDelaySum, numSecurityDelays, carrierDelaySum, numCarrierDelays, name, flightCount);
		if(key.toString().charAt(0) == 'A') {
			airports.add(ds);
			if(flightCount > 50000) busyAirports.add(ds);
			if(flightCount > 100000) busiestAirports.add(ds);
		}else {
			carriers.add(ds);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		airports.sort(Comparator.comparingDouble(DelaySummarizeSorter::getLateDelayPercent));
		busyAirports.sort(Comparator.comparingDouble(DelaySummarizeSorter::getLateDelayPercent));
		busiestAirports.sort(Comparator.comparingDouble(DelaySummarizeSorter::getLateDelayPercent));
		carriers.sort(Comparator.comparingDouble(DelaySummarizeSorter::getCarrierDelayPercent));
		context.write(new Text("Airports causing least and most late aircraft with at least 1000 flights"), NullWritable.get());
		writeTopAirports(airports, 10, context);
		context.write(new Text("\nAirports causing least and most late aircraft with at least 50000 flights"), NullWritable.get());
		writeTopAirports(busyAirports, 10, context);
		context.write(new Text("\nAirports causing least and most late aircraft with at least 100000 flights"), NullWritable.get());
                writeTopAirports(busiestAirports, 10, context);
		context.write(new Text("\nCarriers causing least and most late aircraft with at least 1000 flights"), NullWritable.get());
		writeTopCarriers(carriers, 10, context);
	}

	private static void writeTopAirports(List<DelaySummarizeSorter> list, int n, Context context) throws IOException, InterruptedException {
		context.write(new Text("Airports causing most delays"), NullWritable.get());
		for(int i = list.size()-1; i > list.size() - 1 - n; i--) {
			DelaySummarizeSorter curr = list.get(i);
			//String output = String.format("%s:  Pct. Flights Delayed %.2f, Weather: %e NAS: %d Security: %d Flights: %d", curr.getName(),
			//		curr.getLateDelayPercent(), curr.getWeatherDelay(), curr.getNasDelay(), curr.getSecDelay(), curr.getNumFlights());
			String output = String.format("%s: Pct. Flights Delayed %.2f, Weather Delay %.2f, NAS Delay %.2f," +
					"Security Delay %.2f, Flight Count: %d", curr.getName(), curr.getLateDelayPercent(), curr.getWeatherDelayPercent(),
					curr.getNASDelayPercent(), curr.getSecDelayPercent(), curr.getNumFlights());
			context.write(new Text(output), NullWritable.get());
		}
		context.write(new Text("Airports causing fewest delays"), NullWritable.get());
		for(int i = 0; i < n; i++) {
			DelaySummarizeSorter curr = list.get(i);
//			String output = String.format("%s:  Pct. Flights Delayed %.2f, Weather: %d NAS: %d Security: %d Flights: %d", curr.getName(),
//					curr.getLateDelayPercent(), curr.getWeatherDelay(), curr.getNasDelay(), curr.getSecDelay(), curr.getNumFlights());
  			String output = String.format("%s: Pct. Flights Delayed %.2f, Weather Delay %.2f, NAS Delay %.2f," +
							"Security Delay %.2f, Flight Count: %d", curr.getName(), curr.getLateDelayPercent(), curr.getWeatherDelayPercent(),
					curr.getNASDelayPercent(), curr.getSecDelayPercent(), curr.getNumFlights());
			context.write(new Text(output), NullWritable.get());
		}
	}

	private static void writeTopCarriers(List<DelaySummarizeSorter> list, int n, Context context) throws IOException, InterruptedException {
		context.write(new Text("Carriers causing most delays"), NullWritable.get());
		for(int i = list.size()-1; i > list.size() - 1 - n; i--) {
			DelaySummarizeSorter curr = list.get(i);
			String output = String.format("%s:  Pct. Flights Delayed %.2f, Carrier: %d Flights: %d", curr.getName(),
					curr.getCarrierDelayPercent(), curr.getCarrierDelay(), curr.getNumFlights());
//			String output = String.format("%s: Pct. Flights Delayed %.2f, Flight Count: %d", curr.getName(), curr.getCarrierDelayPercent(), curr.getNumFlights());
			context.write(new Text(output), NullWritable.get());
		}
		context.write(new Text("Carriers causing fewest delays"), NullWritable.get());
		for(int i = 0; i < n; i++) {
			DelaySummarizeSorter curr = list.get(i);
			String output = String.format("%s:  Pct. Flights Delayed %.2f, Carrier: %d Flights: %d", curr.getName(), curr.getCarrierDelayPercent(),
					curr.getCarrierDelay(), curr.getNumFlights());
//			String output = String.format("%s: Pct. Flights Delayed %.2f", curr.getName(), curr.getCarrierDelayPercent());
			context.write(new Text(output), NullWritable.get());
		}
	}
}
