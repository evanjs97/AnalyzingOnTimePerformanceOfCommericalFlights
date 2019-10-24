package hadoop.util;

public class IntDoubleWordCount implements Comparable<IntDoubleWordCount>{

	private String word;
	private double sum;
	private int count;

	public String getWord() {
		return word;
	}

	public int getCount() {
		return count;
	}

	public double getSum() { return sum; }

	public IntDoubleWordCount(String word, double sum, int count) {
		this.word = word;
		this.sum = sum;
		this.count = count;
	}

	@Override
	public int compareTo(IntDoubleWordCount o) {
		int val = Integer.compare(count, o.count);
		return val == 0 ? Double.compare(sum, o.sum) : val;
	}
}
