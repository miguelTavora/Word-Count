package cdle.wordcount.lib;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class WordCountUtils {

	private static final Log log = LogFactory.getLog( WordCountUtils.class );
	
	public static enum Statistics { 
		TotalWords, 
		Distincts, 
		Singletons };

	public static void printPercentageSingletons(long totalWord, long singletons) {
		double result = (double) singletons/totalWord;
		result = result * 100; // this is in percentage
		System.out.println("Percentage of n-grams that occurs once: "+result+"%");
	}
}
