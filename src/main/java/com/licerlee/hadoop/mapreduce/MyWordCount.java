package com.licerlee.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MyWordCount {

    public static void main(String[] args) throws Exception {

        if(args.length < 2){
            System.exit(0);
        }
        String input = args[0];
        String outPut = args[1];

        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance(configuration);

        job.setJarByClass(MyWordCount.class);
        job.setJobName("word counter job");

        Path inputFile = new Path(input);
        TextInputFormat.addInputPath(job, inputFile);

        Path outputFile = new Path(outPut);
        if (outputFile.getFileSystem(configuration).exists(outputFile)) {
            outputFile.getFileSystem(configuration).delete(outputFile, true);
        }
        TextOutputFormat.setOutputPath(job, outputFile);

//        job.setInputPath(new Path("in"));
//        job.setOutputPath(new Path("out"));


        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);


    }
}
