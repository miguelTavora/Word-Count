package cdle.wordcount.mr;

import java.io.File;
import java.io.IOException;

import java.util.*;


import cdle.wordcount.lib.WordCountUtils;
import cdle.wordcount.patterns.Patterns;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper
			extends Mapper<Object, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();

	// variable to change the content of the file
	private boolean caseSensitive = false;
	private Set<String> patternsToSkip = new HashSet<String>();
	private int nGrams = 1;

	private String[] previousContent = null;


	@Override
	public void setup(Context context) throws IOException {

		Configuration conf = context.getConfiguration();

		this.caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);

		boolean skipPatterns = conf.getBoolean( "wordcount.skip.patterns", false);

		this.nGrams = conf.getInt( "wordcount.ngrams", 1);

		// only obtain the patterns to skip if configured that way
		if(skipPatterns) {
			patternsToSkip.addAll(Arrays.asList(Patterns.PATTERNS));
		}

	}



	@Override
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		// content in lower or higher case
		String content = ( this.caseSensitive) ? value.toString() : value.toString().toLowerCase();

		for (String pattern : this.patternsToSkip ) {
			content = content.replaceAll(pattern, "" );
		}

		// variable to print if has used cache or not
		String result = getCacheFiles(context);
		System.out.println("Cache used: "+result);

		//String[] grams = getNGrams(content, this.nGrams);
		String[] grams = getAllNGrams(content, this.nGrams);

		for (String gram : grams ) {
			word.set(gram);
			context.write(word, one);

			context.getCounter(WordCountUtils.Statistics.TotalWords).increment(1);
		}
	}


	private String[] getAllNGrams(String input, int n) {
		//String[] tokens = input.split("\\s+");
		String[] tokens = getTokens(input);

		// first state, when ot does not have any data
		if (previousContent == null) {
			// when it has not enough length just set the values
			if (tokens.length < n) {
				this.previousContent = tokens;
				return new String[0];
			}
			// when has enough size, generate n-grams and copy the last words
			else {
				String[] nGrams =  getNGrams(input, n);
				this.previousContent = copyPrevious(tokens, n);
				return nGrams;
			}
		}

		// generate a string with all the previous content
		String previous =getUnifiedString(this.previousContent);

		// adds to the new one
		String fullStrg = previous+input;

		// must create a new tokens because has new content added
		String[] newTokens = fullStrg.split("\\s+");

		// when it's not enough on the second or more to full a gram
		if(newTokens.length < n) {
			// just copy the words that exists
			this.previousContent = copyPrevious(newTokens, n);
			return new String[0];
		}

		// when it has enough content, gets the previous and the new
		else {
			// calculates the n-grams
			String[] nGrams = getNGrams(fullStrg, n);
			// copy the new previous
			this.previousContent = copyPrevious(newTokens, n);
			return nGrams;
		}

	}

	private String[] getNGrams(String input, int n) {
		//String[] tokens = input.split("\\s+");
		String[] tokens = getTokens(input);

		String[] grams = new String[tokens.length - n + 1];

		for (int i = 0; i < tokens.length - n + 1; i++) {
			String gram = "";
			for (int j = 0; j < n; j++) {
				gram += tokens[i + j] + " ";
			}
			grams[i] = gram.trim();
		}
		return grams;
	}

	private String[] copyPrevious(String[] tokens, int n) {
		// when size is 0, just return empty
		if(tokens.length == 0) return new String[0];

		// when size is less than n - 1 it's just the tokens
		if(tokens.length < n - 1) return tokens;

		// when has enough size
		String[] previous = new String[n - 1];
		int count = 0;
		for(int i = n - 1; i > 0; i--) {
			previous[count] = tokens[tokens.length-i];
			count++;
		}
		return previous;

	}

	private String getUnifiedString(String[] previous) {
		String result = "";
		for(int i = 0; i < previous.length; i++) {
			result += previous[i]+" ";
		}
		return result;
	}

	private String[] getTokens(String input) {
		System.out.println("Using new TOkens!!!");
		StringTokenizer st = new StringTokenizer(input);
		String[] tokens = new String[st.countTokens()];

		int i = 0;
		while (st.hasMoreTokens()) {
			tokens[i] = st.nextToken();
			i++;
		}
		return tokens;
	}

	@SuppressWarnings("deprecation")
	private String getCacheFiles(Context context) throws IOException {
		// used to obtain all the directories used in cache
		String result = "";
		Path[] localCacheFiles = context.getLocalCacheFiles();
		if (localCacheFiles != null) {

			for (Path localCacheFile : localCacheFiles) {
				System.out.println(localCacheFile);
				result += " " + localCacheFile;
			}
		}
		else
			result = "--------NÃO_USADO-----------";

		return result;
	}
}









