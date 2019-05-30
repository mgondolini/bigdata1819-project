package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;


public class Job2 {

	public static class CategoryMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text category = new Text();
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, IntWritable value, Context context) throws IOException, InterruptedException {
			/**
			 * MAP-1
			 * category:tag | 1
			 *
			 * REDUCE-1
			 * category:tag | total
			 *
			 * MAP-2
			 * category | tag:total
			 *
			 * REDUCE-2
			 * category | 10 tag più presenti
			 */

			String line = value.toString();
			String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 11);
			if(tokens.length > 7){
				if(tokens[4].equals("category_id") || tokens[6].equals("tags")) {
					return;
				}else {
					String[] tags = tokens[6].split("(\\|)");
					for (String tag : tags) {
						category.set(tokens[4].concat(":"+tag));
						context.write(category, one);
					}
				}
			}else return;
		}
	}

	public static class TagReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job2");

		job.setJarByClass(Job2.class);
		job.setMapperClass(CategoryMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(TagReducer.class);

		FileSystem fs = FileSystem.get(new Configuration());
		Path outputPath = new Path(args[0]);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CategoryMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, CategoryMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, CategoryMapper.class);

		//MI 4 con categorie json

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
