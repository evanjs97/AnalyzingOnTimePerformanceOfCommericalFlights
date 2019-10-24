import hadoop.combiner.BusiestAirportCombiner;
import hadoop.combiner.CarrierDelayCombiner;
import hadoop.combiner.MinimizeDelayCombiner;
import hadoop.combiner.WeatherDelayCombiner;
import hadoop.mapper.*;
import hadoop.reducer.*;
import hadoop.util.AverageDelayWritable;
import hadoop.util.DoubleTextWritable;
import hadoop.util.IntTextWritable;
import hadoop.util.LateAircraftWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Entry {


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, args[0]);
		job.setJarByClass(Entry.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputDirRecursive(job, true);
//		FileInputFormat.setInputPaths(job, new Path(args[1]));

		FileOutputFormat.setOutputPath(job, new Path(args[4]));

		switch (args[0]) {
			case "delays":
				runMinMaxDelays(job, args);
				break;
			case "busiest":
				runBusiestAirports(job, args);
				break;
			case "weather":
				runWeatherDelays(job, args);
				break;
			case "carriers":
				runCarrierDelays(job, args);
				break;
			case "coast":
				runCoastDelays(job, args);
				break;
			case "late":
				runLateAircraftDelays(job, args);
				break;
		}
//		job.setCombinerClass(TaskSevenCombiner.class);
//		job.setReducerClass(TaskSevenReducer.class);
//		job.setMapperClass(TaskSevenMapper.class);
//
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setMapOutputKeyClass(NullWritable.class);
//		job.setMapOutputValueClass(SongSegment.class);
//		job.setOutputKeyClass(NullWritable.class);
//		job.setOutputValueClass(Text.class);

//		FileInputFormat.addInputPath(job, new Path(analysis));
//		FileOutputFormat.setOutputPath(job, new Path(output));
//		FileInputFormat.setInputDirRecursive(job, true);
		job.waitForCompletion(true);
	}

	public static void runBusiestAirports(Job job, String[] args) throws IOException {
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BusiestAirportMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, AirportCodeJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntTextWritable.class);
		job.setCombinerClass(BusiestAirportCombiner.class);
		job.setReducerClass(BusiestAirportReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	}

	public static void runMinMaxDelays(Job job, String[] args) throws IOException {
		job.setMapperClass(MinimizeDelayMapper.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AverageDelayWritable.class);
		job.setCombinerClass(MinimizeDelayCombiner.class);
		job.setReducerClass(MinimizeDelayReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}

	public static void runWeatherDelays(Job job, String[] args) throws IOException {
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, WeatherDelayMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, WeatherDelayMapperHelper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AverageDelayWritable.class);
		job.setCombinerClass(WeatherDelayCombiner.class);
		job.setReducerClass(WeatherDelayReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
	}

	public static void runCarrierDelays(Job job, String[] args) throws IOException {
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CarrierDelayMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, CarrierDelayMapperHelper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AverageDelayWritable.class);
		job.setCombinerClass(CarrierDelayCombiner.class);
		job.setReducerClass(CarrierDelayReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}

	public static void runCoastDelays(Job job, String[] args) throws IOException {
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, EastWestMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, EastWestCoastMapperHelper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AverageDelayWritable.class);
		job.setCombinerClass(WeatherDelayCombiner.class);
		job.setReducerClass(EastWestCoastReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
	}

	public static void runLateAircraftDelays(Job job, String[] args) throws IOException {
		job.setMapperClass(LateAircraftMapper.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LateAircraftWritable.class);
		job.setReducerClass(LateAircraftReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
	}
}
