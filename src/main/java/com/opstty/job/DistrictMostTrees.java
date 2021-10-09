package com.opstty.job;

import com.opstty.mapper.DistrictMapper;
import com.opstty.mapper.DistrictMostTreesMapper;
import com.opstty.mapper.TokenizerMapper;
import com.opstty.reducer.DistrictMostTreesReducer;
import com.opstty.reducer.DistrictReducer;
import com.opstty.reducer.IntSumReducer;
import com.opstty.writable.DistrictCountWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistrictMostTrees {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: districtmostTrees <in> <out> [<out>...]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "districtrees");
        job.setJarByClass(DistrictTrees.class);
        job.setMapperClass(DistrictMapper.class);
        job.setCombinerClass(DistrictReducer.class);
        job.setReducerClass(DistrictReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "districtmostTrees");
        job2.setJarByClass(DistrictMostTrees.class);
        job2.setMapperClass(DistrictMostTreesMapper.class);
        job2.setCombinerClass(DistrictMostTreesReducer.class);
        job2.setReducerClass(DistrictMostTreesReducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(DistrictCountWritable.class);

        // Get output from the job1
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));


        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
