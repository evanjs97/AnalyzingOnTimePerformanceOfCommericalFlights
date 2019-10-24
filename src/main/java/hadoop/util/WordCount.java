package hadoop.util;

public class WordCount implements Comparable<WordCount>{
	private String word;
	private int count;

	public String getWord() {
		return word;
	}

	public int getCount() {
		return count;
	}

	public WordCount(String word, int count) {
		this.word = word;
		this.count = count;
	}

	@Override
	public int compareTo(WordCount other) {
		return Integer.compare(count, other.count);
	}
}
