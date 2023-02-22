package cdle.wordcount.lib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;


public class ZipFile {


    public void convertToZip(Configuration conf, String input) throws IOException {

        boolean foundZip = hasZipFiles(conf, input, "zip");

        if (foundZip) {
            this.setCompressionZip(conf, input);
        }
    }

    public void setCompressionZip(Configuration conf, String input) {

        System.out.println("Conveteu para ZIP!!!!");
        // to zip files
        conf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        conf.set("mapreduce.input.fileinputformat.input.dir", input);
        conf.set("mapreduce.input.fileinputformat.input.dir.filter","*.zip");
        conf.set("mapreduce.compress.map.output","true");
        conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.ZipCodec");
    }

    public boolean hasZipFiles(Configuration conf, String directoryPath, String fileType) throws IOException {

        try {
            // first he tries on the HDFS file system
            System.out.println("TRY!!!!!");

            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(directoryPath);

            FileStatus[] fileStatuses = fs.listStatus(path);

            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.getPath().getName().endsWith(".zip")) {
                    return true;
                }
            }
            return false;

        } catch (Exception e) {
            // if he can't find the dir it goes to the local file system
            System.out.println("CATCH!!!!!");
            return this.checkFileExistsWithExtension(directoryPath, fileType);
        }
    }

    public boolean checkFileExistsWithExtension(String directoryPath, String extension) {
        directoryPath = directoryPath.substring(6)+"/"; // remove file://

        File dir = new File(directoryPath);

        FileFilter fileFilter = new FileFilter() {
            public boolean accept(File file) {
                return file.getName().endsWith("." + extension);
            }
        };

        File[] files = dir.listFiles(fileFilter);
        return files != null && files.length > 0;
    }


    /*public static String convertToPlainText(Configuration conf) throws IOException {

        checkFile("", "");

        //Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/home/usermr/examples/input//wikipedia/enwiki-2019-04-0000000243.zip");
        FSDataInputStream inputStream = fs.open(path);
        Scanner scanner = new Scanner(inputStream, "UTF-8").useDelimiter("\\A");
        String text = scanner.hasNext() ? scanner.next() : "";
        scanner.close();
        inputStream.close();
        return text;
    }*/

    /*public static String convertToPlainText(Configuration conf) throws IOException {
        /*Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //Path hdfsFilePath = new Path();
        File localFile = new File("/home/usermr/examples/input//wikipedia/enwiki-2019-04-0000000243.zip");
        //FileUtil.copy(fs, hdfsFilePath, localFile, false, conf);

        Scanner scanner = new Scanner(localFile, "UTF-8").useDelimiter(" ");
        String text = scanner.hasNext() ? scanner.next() : "";
        System.out.println("RESULTADO OBTIDO::::::: "+ );
        scanner.close();
        return text;
    }*/
}
