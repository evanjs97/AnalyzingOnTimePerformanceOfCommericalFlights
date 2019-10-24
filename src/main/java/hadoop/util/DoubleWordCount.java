package hadoop.util;

public class DoubleWordCount implements Comparable<DoubleWordCount>{
	private String word;
	private double count;

	public String getWord() {
		return word;
	}

	public double getCount() {
		return count;
	}

	public DoubleWordCount(String word, double count) {
		this.word = word;
		this.count = count;
	}

	@Override
	public int compareTo(DoubleWordCount other) {
		return Double.compare(count, other.count);
	}
}
