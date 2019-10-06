package lab1.lab1;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class task2_toprating {

    public static class toprating_Mapper
            extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private final static FloatWritable irating = new FloatWritable();
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
    			throws IOException, InterruptedException {
    		
    		String line = value.toString();
    		if(line.length()>0) {
    		String colsplit[] = line.split("\t");
    		word.set(colsplit[0]);
    		if(colsplit.length>6){
    		if(colsplit[6].matches("\\d+.+")){
    			float f=Float.parseFloat(colsplit[6]);
    			irating.set(f);
    		}
    		}
    		context.write(word, irating);
    		
    	}
    }
    }

    public static class toprating_Reducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
    			throws IOException, InterruptedException {
    		float total=0;
    		int i=0;
    		for(FloatWritable value:values){
    			total=total+value.get();
    			i=i+1;
    		}
    		total=total/i;
    		FloatWritable itotal=new FloatWritable(total);
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
       
		Job job = new Job(conf, "toprating");
        job.setJarByClass(App.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
 
        job.setMapperClass(toprating_Mapper.class);
        job.setReducerClass(toprating_Reducer.class);
 
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
        			"cat /home/cloudera/Desktop/BDP/Lab1/task2op/part-r-00000 | sort -n -k2 -r | head -n10"
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