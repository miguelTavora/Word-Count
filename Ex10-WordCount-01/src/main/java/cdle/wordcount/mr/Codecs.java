package cdle.wordcount.mr;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import java.io.File;


public class Codecs {

    private final String LOCAL_PATH = "./";

    private final String CONFIG_FILE_MAPREDUCE = "configuration-mapreduce.xml";
    private final String CONFIG_FILE_WORDCOUNT = "configuration-wordcount.xml";


    public void setCodec(Configuration conf) {

        System.out.println( "Adding resource from first configuration..." );

        // obtains the configuration to the map reduce
        if(checkLocalFile(LOCAL_PATH, CONFIG_FILE_MAPREDUCE)) {
            conf.addResource( new Path( LOCAL_PATH, CONFIG_FILE_MAPREDUCE ) );
        }

        // obtains the config to the word count
        if(checkLocalFile(LOCAL_PATH, CONFIG_FILE_WORDCOUNT)) {
            conf.addResource( new Path( LOCAL_PATH, CONFIG_FILE_WORDCOUNT ) );
        }

        String codedUsed2 = conf.get("mapreduce.output.fileoutputformat.compress.codec");
        //String codedUsed2 = conf.get("io.compression.codecs");
        //String codedUsed3 = conf.get("mapred.output.compression.codec");

        System.out.println("Codec used: "+ codedUsed2);
        System.out.println("Sensitive case: " + conf.getBoolean("wordcount.case.sensitive", true));
        System.out.println("N-grams: " + conf.getInt( "wordcount.ngrams", 1));
        System.out.println("Skip patterns: "+conf.getBoolean( "wordcount.skip.patterns", false));
    }


    public boolean checkLocalFile(String path, String fileName) {

        File file = new File(path +"/" +fileName);
        if (file.exists()) {
            System.out.println("Ficheiro de configuração existe !!");
            return true;
        } else {
            System.out.println("Ficheiro de configuração NÃOOOOO existe !!!!!!");
            return false;
        }
    }

}
