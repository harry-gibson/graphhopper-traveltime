package uk.ac.ox.map.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class IsochroneDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if ((remainingArgs.length != 2)) {
            System.err.println("Usage: IsochroneDriver <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "IsochroneDriver");
        job.setJarByClass(IsochroneDriver.class);

        job.setMapperClass(OriginMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);


        job.setNumReduceTasks(0);


        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
