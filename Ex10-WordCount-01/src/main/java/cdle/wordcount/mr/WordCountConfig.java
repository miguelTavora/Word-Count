package cdle.wordcount.mr;

import cdle.wordcount.lib.WordCountUtils;
import cdle.wordcount.lib.ZipFile;
import cdle.wordcount.sort.SortWordMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;

public class WordCountConfig extends Configured
        implements Tool {

    private final String DIR_SORTED = "/sorted";


    @Override
    public int run(String[] args) throws Exception {

        // set the compression out type
        Codecs codecs = new Codecs();
        codecs.setCodec( getConf() );


        // zip file to test if the directory has zip files
        ZipFile zipFile = new ZipFile();
        zipFile.convertToZip(getConf(), args[0]);

        // create the job
        Job job = Job.getInstance( getConf() );

        // set jar and name
        job.setJarByClass( WordCountApplication.class );
        job.setJobName( "Word Count" );

        // command to cache the different files
        job.addCacheFile(new URI(args[0]));

        // check the files stored in cache
        getCacheFiles(job);

        // check the number of reducers used
        int numReducers = job.getNumReduceTasks();
        System.out.println("Number of reducers: " + numReducers);



        FileInputFormat.addInputPath(job, new Path(args[0]) );
        FileOutputFormat.setOutputPath(job, new Path(args[1]) );


        job.setMapperClass( WordCountMapper.class );
        job.setReducerClass( WordCountReducer.class );


        System.out.println("-----------------------------------------------------------");

        // Output types of map function
        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( IntWritable.class );


        // Output types of reduce function
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( IntWritable.class );


        boolean result = job.waitForCompletion( false );

        if ( result == true ) {

            Counter distinctsCounter = job.getCounters().findCounter( WordCountUtils.Statistics.Distincts );
            Counter singletonsCounter = job.getCounters().findCounter( WordCountUtils.Statistics.Singletons );
            Counter totalCounter = job.getCounters().findCounter( WordCountUtils.Statistics.TotalWords );

            System.out.printf( "Number of distinct n-grams: %d\n", distinctsCounter.getValue() );
            System.out.printf( "Number of singletons n-grams: %d\n", singletonsCounter.getValue() );
            System.out.printf( "Number of total n-grams: %d\n", totalCounter.getValue() );

            WordCountUtils.printPercentageSingletons(totalCounter.getValue(),singletonsCounter.getValue());
        }
        else {
            System.out.printf( "Job failed!\n" );
        }

        boolean resultSort = jobSortByOccurrences(args);

        return result && resultSort ? 0 : 1;

    }

    public boolean jobSortByOccurrences(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance( getConf() );
        job.setJarByClass( WordCountApplication.class );
        job.setJobName( "Sort Word" );

        // check the number of reducers used
        int numReducers = job.getNumReduceTasks();
        System.out.println("Number of reducers: " + numReducers);


        FileInputFormat.addInputPath(job, new Path(args[1]) );
        FileOutputFormat.setOutputPath(job, new Path(args[1]+DIR_SORTED) );

        job.setMapperClass( SortWordMapper.class );

        System.out.println("-----------------------------------------------------------");


        job.setMapOutputKeyClass( LongWritable.class );
        job.setMapOutputValueClass( Text.class );


        boolean result = job.waitForCompletion( false );

        return result;
    }

    // check the files stored in cache
    public static void getCacheFiles(Job job) throws IOException {

        URI[] cacheFiles = job.getCacheFiles();
        if (cacheFiles != null) {
            System.out.println("Cache files:");
            for (URI cacheFile : cacheFiles) {
                System.out.println(cacheFile);
            }
        }
        else
            System.out.println("Não tem métodos em cache!!!!!");
    }
}
