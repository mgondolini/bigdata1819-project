package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

			if (tokens[4].equals("category_id") || tokens[6].equals("tags")) {
				return;
			}

			String[] tags = tokens[6].split("(\\|)");
			for (String tag : tags) {
				categoryTagKey.set(tokens[4].concat(":" + tag));
				context.write(categoryTagKey, one);
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

			String[] tokens = key.toString().split(":", 2);

			category.set(tokens[0]);

			tagCount.set(tokens[1] + ":" + value.toString());

			context.write(category, tagCount);
		}
	}

	public static class TagReducer extends Reducer<Text, Text, Text, Text>{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			TreeMap<Double, String> tagWithOccurences = new TreeMap<>();

			for(Text t: values){
				String[] valueTokens = t.toString().split(":", 3);
				try {
					double occurences = Double.parseDouble(valueTokens[1]);
					tagWithOccurences.put(occurences, valueTokens[0]);
				} catch (Exception e) { }
			}

			if (tagWithOccurences.size() > 10) {
				Double lastKeyToKeep = (Double) tagWithOccurences.descendingMap().keySet().toArray()[10];
				Map<Double, String> mostUsedTags = tagWithOccurences.descendingMap().headMap(lastKeyToKeep);
				context.write(key, new Text(mostUsedTags.toString()));
			} else {
				context.write(key, new Text(tagWithOccurences.toString()));
			}

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		ArrayList<Job> jobs = new ArrayList<>();
		jobs.add(Job.getInstance(conf, "First Job"));
		jobs.add(Job.getInstance(conf, "Second Job"));

		for (Job job: jobs) job.setJarByClass(Job2Alternative.class);

		// JOB 1

		jobs.get(0).setMapperClass(Job2Alternative.CategoryMapper.class);
		jobs.get(0).setMapOutputKeyClass(Text.class);
		jobs.get(0).setMapOutputValueClass(IntWritable.class);
		jobs.get(0).setReducerClass(Job2Alternative.CategoryReducer.class);
		jobs.get(0).setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(jobs.get(0), new Path(args[1]), TextInputFormat.class, Job2Alternative.CategoryMapper.class);
		MultipleInputs.addInputPath(jobs.get(0), new Path(args[2]), TextInputFormat.class, Job2Alternative.CategoryMapper.class);
		MultipleInputs.addInputPath(jobs.get(0), new Path(args[3]), TextInputFormat.class, Job2Alternative.CategoryMapper.class);

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
