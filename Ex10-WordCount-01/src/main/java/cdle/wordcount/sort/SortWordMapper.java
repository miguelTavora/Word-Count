package cdle.wordcount.sort;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortWordMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private final static LongWritable occurrences = new LongWritable();


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        System.out.println("value: "+ value);

        String[] elements = value.toString().split("\\s+");
        System.out.println("Elements: "+ Arrays.toString(elements));

        String[] wordAndCount = obtainWordAndCount(elements);
        System.out.println("wordAndCount: "+ Arrays.toString(wordAndCount));

        long count = Long.parseLong(wordAndCount[1]);


        occurrences.set(count);
        context.write(new LongWritable(count), new Text(wordAndCount[0]));
    }

    // returns an array where first element is the word and second is the number of occurrences
    private String[] obtainWordAndCount(String[] elements) {
        String wordResult = "";
        for(int i = 0; i < elements.length-1; i++) {
            if (i != elements.length-2)
                wordResult += elements[i]+" ";
            else
                wordResult += elements[i];
        }
        String[] result = {wordResult, elements[elements.length-1]};
        return result;
    }
}
