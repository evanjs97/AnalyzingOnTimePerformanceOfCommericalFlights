package hadoop.reducer;

import hadoop.util.IntTextWritable;
import hadoop.util.WordCount;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import hadoop.util.Utils;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.util.TreeSet;

public class BusiestAirportReducer extends Reducer<Text, IntTextWritable, Text, IntWritable> {
//	public void reduce(Text key, Iterable<IntTextWritable> values, Context context) throws IOException, InterruptedException {
//		int count = 0;
//		String name = "";
//		for(IntTextWritable value : values) {
//			count++;
//			if(!value.getName().toString().isEmpty()) name = value.getName().toString();
//		}
//		context.write(new Text(key + ":" + name), new IntWritable(count));
//	}

	protected TreeSet<WordCount> tree = new TreeSet<>();
	protected HashMap<Integer, HashMap<String, Integer>> years = new HashMap<>();
	protected HashMap<String, String> codeMapping = new HashMap<>();

	public void reduce(Text key, Iterable<IntTextWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		String text = "";
		for(IntTextWritable value : values) {
			count += value.getCount().get();
			if(!value.getName().toString().isEmpty()) {
				if(Utils.isNumber(value.getName().toString())) {
					int year = Integer.parseInt(value.getName().toString());
					HashMap<String, Integer> counts = years.remove(year);
					if(counts == null) counts = new HashMap<>();
					Integer c = counts.remove(key.toString());
					if(c == null) c = value.getCount().get();
					else c+=value.getCount().get();
					
					counts.put(key.toString(), c);
					years.put(year, counts);
				}
				else if(text.isEmpty()) {
					text = value.getName().toString();
				}
			}
		}
		if(!text.isEmpty()) {
			codeMapping.put(key.toString(), text);
			tree.add(new WordCount(text, count));
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		int counter = 0;
		while(!tree.isEmpty() && counter < 10) {
			WordCount current = tree.pollLast();
			context.write(new Text(Utils.reformatString(current.getWord(), 50)), new IntWritable(current.getCount()));
			counter++;
		}
		
		for(Map.Entry<Integer, HashMap<String, Integer>> entry : years.entrySet()) {
			List<Map.Entry<String, Integer>> list = new ArrayList<>(entry.getValue().entrySet());
			list.sort(Comparator.comparingInt(Map.Entry::getValue));
			context.write(new Text("YEAR"), new IntWritable(entry.getKey()));
			for(int i = list.size()-1; i > list.size() - 11; i--) {
				String name = codeMapping.get(list.get(i).getKey());
				context.write(new Text(list.get(i).getKey()+" "+Utils.reformatString(name, 50)), new IntWritable(list.get(i).getValue()));
			}
		}
	}
}
