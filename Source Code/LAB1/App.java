package lab1.lab1;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App {

    public static class Top5Cat_Mapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable ione = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
    			throws IOException, InterruptedException {
    		
    		String line = value.toString();
    		String colsplit[] = line.split("\t");
    		if(colsplit.length>5)
    		word.set(colsplit[3]);
    		context.write(word, ione);
    		
    	}
    }

    public static class Top5Cat_Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
    			throws IOException, InterruptedException {
    		int total=0;
    		for(IntWritable value:values){
    			total=total+value.get();
    		}
    		IntWritable itotal=new IntWritable(total);
    		context.write(key,itotal);
    	}
    }


public static boolean deleteDir(File dir) {
    if (dir.isDirectory()) {
        String[] children = dir.list();
        for (int i=0; i<children.length; i++) {
            boolean success = deleteDir(new File(dir, children[i]));
            if (!success) {
                return false;
            }
        }
    }
    return dir.delete();
}


    public static void main(String[] args) throws Exception {
    	
    	File file = new File(args[1]);
    	deleteDir(file);
    	Configuration conf = new Configuration();
       
		Job job = new Job(conf, "Top5Category");
        job.setJarByClass(App.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        job.setMapperClass(Top5Cat_Mapper.class);
        job.setReducerClass(Top5Cat_Reducer.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.waitForCompletion(true);
        
        String s;
        Process p;
        try {
        	String[] cmd = {
        			"/bin/sh",
        			"-c",
        			"cat /home/cloudera/Desktop/BDP/Lab1/task1op/part-r-00000 | sort -n -k2 -r | head -n5"
        			};
            p = Runtime.getRuntime().exec(cmd);
            BufferedReader br = new BufferedReader(
                new InputStreamReader(p.getInputStream()));
            while ((s = br.readLine()) != null)
                System.out.println("line: " + s);
            p.waitFor();
            System.out.println ("exit: " + p.exitValue());
            p.destroy();
        } catch (Exception e) {}
        
    }
}