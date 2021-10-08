package com.opstty.job;

import com.opstty.mapper.DistrictMostTreesMapper;
import com.opstty.mapper.TokenizerMapper;
import com.opstty.reducer.DistrictMostTreesReducer;
import com.opstty.reducer.IntSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictMostTrees {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: districtmostTrees <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "districtmostTrees");
        job.setJarByClass(DistrictMostTrees.class);
        job.setMapperClass(DistrictMostTreesMapper.class);
        job.setCombinerClass(DistrictMostTreesReducer.class);
        job.setReducerClass(DistrictMostTreesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);


        Job job2 = Job.getInstance(conf, "districtmostTrees2");
        job2.setJarByClass(DistrictMostTrees.class);
        job2.setMapperClass(DistrictMostTreesMapper.class);
        job2.setCombinerClass(DistrictMostTreesReducer.class);
        job2.setReducerClass(DistrictMostTreesReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // Get output from the job1
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));



        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
