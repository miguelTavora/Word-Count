package cdle.wordcount.mr;

import org.apache.hadoop.util.ToolRunner;


public class WordCountApplication {

	
	public static void main(String[] args) throws Exception {

		if ( args.length < 2 ) {
			System.err.println( "hadoop ... <input path> <output path>" );
			System.exit(-1);
		}

		System.exit( ToolRunner.run( new WordCountConfig(), args) );

	}

}
