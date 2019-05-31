package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;


public class Job2 {

	public static class CategoryMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text categoryTagKey = new Text();
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 11);
			if(tokens[4].equals("category_id") || tokens[6].equals("tags")) {
				return;
			}else {
				String[] tags = tokens[6].split("(\\|)");
				for (String tag : tags) {
					categoryTagKey.set(tokens[4].concat(":"+tag));
					context.write(categoryTagKey, one);
				}
			}
		}
	}

	public static class CategoryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

	public static class TagMapper extends Mapper<Text, Text, Text, Text> {
		private Text category = new Text();
		private Text tagCount = new Text();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = key.toString().split(":");
			category.set(tokens[0]);
			tagCount.set(tokens[1] + " "+ value.toString());
			context.write(category, tagCount);
		}
	}

	public static class TagReducer extends Reducer<Text, Text, Text, ArrayList<Text>> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<Text> categoryTags = new ArrayList<>();
			for(Text t: values){
				categoryTags.add(t);
			}
			context.write(key, categoryTags);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("key.value.separator.in.input.line", "\t");
		ArrayList<Job> jobs = new ArrayList<>();
		jobs.add(Job.getInstance(conf, "First job"));
		jobs.add(Job.getInstance(conf, "Second job"));

		// Output path and data type of first job
		// should be the same as the input ones of the second job
		for (Job job: jobs) job.setJarByClass(Job2.class);

		jobs.get(0).setMapperClass(CategoryMapper.class);
		jobs.get(0).setMapOutputKeyClass(Text.class);
		jobs.get(0).setMapOutputValueClass(IntWritable.class);
		jobs.get(0).setReducerClass(CategoryReducer.class);
		jobs.get(0).setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(jobs.get(0), new Path(args[1]), TextInputFormat.class, CategoryMapper.class);
		MultipleInputs.addInputPath(jobs.get(0), new Path(args[2]), TextInputFormat.class, CategoryMapper.class);
		MultipleInputs.addInputPath(jobs.get(0), new Path(args[3]), TextInputFormat.class, CategoryMapper.class);

		FileSystem fs = FileSystem.get(new Configuration());
		Path firstJobOutputPath = new Path(args[0]);
		if (fs.exists(firstJobOutputPath)) {
			fs.delete(firstJobOutputPath, true);
		}
		FileOutputFormat.setOutputPath(jobs.get(0), firstJobOutputPath);

		FileInputFormat.addInputPath(jobs.get(1), firstJobOutputPath);
		Path finalJobOutputPath = new Path(args[4]);
		if (fs.exists(finalJobOutputPath)) {
			fs.delete(finalJobOutputPath, true);
		}
		FileOutputFormat.setOutputPath(jobs.get(1), finalJobOutputPath);
		jobs.get(1).setInputFormatClass(KeyValueTextInputFormat.class);
		jobs.get(1).setMapperClass(TagMapper.class);
		jobs.get(1).setMapOutputKeyClass(Text.class);
		jobs.get(1).setMapOutputValueClass(Text.class);
		jobs.get(1).setReducerClass(TagReducer.class);

		for (Job job: jobs) {
			if (!job.waitForCompletion(true)) {
				System.exit(1);
			}
		}

	}
}
