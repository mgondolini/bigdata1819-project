package job2;

import com.cloudera.org.codehaus.jackson.JsonFactory;
import com.cloudera.org.codehaus.jackson.JsonParser;
import com.cloudera.org.codehaus.jackson.JsonToken;
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


public class TopTagsInVideosCategories {

	private final static String SPLIT_REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
	private final static int FIELDS_NUMBER = 11;

	public static class CategoryTagMapper extends Mapper<Object, Text, Text, IntWritable> {

		private Text categoryTagKey = new Text();
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(SPLIT_REGEX, FIELDS_NUMBER);

			if (tokens[4].equals("category_id") || tokens[6].equals("tags")) {
				return;
			}

			String[] tags = tokens[6].split("(\\|)");
			for (String tag : tags) {
				if(!tag.equals("[none]")) {
					categoryTagKey.set(tokens[4].concat(":" + tag));
					context.write(categoryTagKey, one);
				}
			}
		}
	}

	public static class CategoryTagReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

	public static class TagReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			TreeSet<String> tagWithOccurrences = new TreeSet<>();

			for(Text t: values){
				String[] valueTokens = t.toString().split(":", 3);
				try {
//					int occurrences = Integer.parseInt(valueTokens[1]);
					tagWithOccurrences.add(valueTokens[1] + "=" + valueTokens[0]);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			ArrayList<String> topTags = new ArrayList<>();

			int n = 0;

			Iterator<String> iterator = tagWithOccurrences.descendingSet().iterator();

			while (n < 10 && iterator.hasNext()) {
				topTags.add(iterator.next());
				n++;
			}

			context.write(key, new Text(topTags.toString()));
		}
	}


	public static class CategoryNameMapper extends Mapper<Object, Text, Text, Text> {

		static JsonFactory factory = new JsonFactory();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			JsonParser parser = factory.createJsonParser(value.toString());
			Text idCategoryString = new Text();
			Text nameCategoryString = new Text();
			while(!parser.isClosed()){
				JsonToken jsonToken = parser.nextToken();

				if(JsonToken.FIELD_NAME.equals(jsonToken)){
					String fieldName = parser.getCurrentName();

					jsonToken = parser.nextToken();

					if("id".equals(fieldName)){
						idCategoryString.set(parser.getText());
					} else if ("category".equals(fieldName)){
						nameCategoryString.set("join-category:" + parser.getText());
					}
				}
			}
			context.write(idCategoryString, nameCategoryString);
		}
	}

	public static class CategoryNameReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String categoryName = "";
			String categoryTags = "";

			for(Text val : values) {
				if (val.toString().contains("join-category")) {
					categoryName = val.toString().split(":", 2)[1];
				} else {
					categoryTags = val.toString();
				}
			}

			context.write(new Text(categoryName), new Text(categoryTags));
		}
	}

	public static class DummyCategoryMapper extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	// parameters:
	// [0] first csv input
	// [1] second csv input
	// [2] third csv input
	// [3] first job output
	// [4] second job output
	// [5] third job output
	// [6] categoryies's name json

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		ArrayList<Job> jobs = new ArrayList<>();
		jobs.add(Job.getInstance(conf, "Category:Tag > Total"));
		jobs.add(Job.getInstance(conf, "Orders tag by occurrences grouped by category"));
		jobs.add(Job.getInstance(conf, "Join category id with category name"));

		for (Job job: jobs){
			job.setJarByClass(Job2Alternative.class);
		}

		// JOB 1
		MultipleInputs.addInputPath(jobs.get(0), new Path(args[0]), TextInputFormat.class, TopTagsInVideosCategories.CategoryTagMapper.class);
		MultipleInputs.addInputPath(jobs.get(0), new Path(args[1]), TextInputFormat.class, TopTagsInVideosCategories.CategoryTagMapper.class);
		MultipleInputs.addInputPath(jobs.get(0), new Path(args[2]), TextInputFormat.class, TopTagsInVideosCategories.CategoryTagMapper.class);

		jobs.get(0).setMapOutputKeyClass(Text.class);
		jobs.get(0).setMapOutputValueClass(IntWritable.class);
		jobs.get(0).setOutputFormatClass(TextOutputFormat.class);
		jobs.get(0).setCombinerClass(CategoryTagReducer.class);
		jobs.get(0).setReducerClass(CategoryTagReducer.class);

		FileSystem fs = FileSystem.get(new Configuration());
		Path firstJobOutputPath = new Path(args[3]);
		if (fs.exists(firstJobOutputPath)) {
			fs.delete(firstJobOutputPath, true);
		}
		FileOutputFormat.setOutputPath(jobs.get(0), firstJobOutputPath);

		// JOB 2
		FileInputFormat.addInputPath(jobs.get(1), firstJobOutputPath);

		jobs.get(1).setInputFormatClass(KeyValueTextInputFormat.class);
		jobs.get(1).setMapperClass(TagMapper.class);
		jobs.get(1).setReducerClass(TagReducer.class);
		jobs.get(1).setMapOutputKeyClass(Text.class);
		jobs.get(1).setMapOutputValueClass(Text.class);

		Path secondOutputPath = new Path(args[4]);
		if (fs.exists(secondOutputPath)) {
			fs.delete(secondOutputPath, true);
		}
		FileOutputFormat.setOutputPath(jobs.get(1), secondOutputPath);

		//JOB 3 (JOIN)
		MultipleInputs.addInputPath(jobs.get(2), secondOutputPath, KeyValueTextInputFormat.class, TopTagsInVideosCategories.DummyCategoryMapper.class);
		MultipleInputs.addInputPath(jobs.get(2), new Path(args[6]), TextInputFormat.class, TopTagsInVideosCategories.CategoryNameMapper.class);

		jobs.get(2).setReducerClass(CategoryNameReducer.class);
		jobs.get(2).setOutputKeyClass(Text.class);
		jobs.get(2).setOutputValueClass(Text.class);

		Path finalOutputPath = new Path(args[5]);
		if (fs.exists(finalOutputPath)) {
			fs.delete(finalOutputPath, true);
		}
		FileOutputFormat.setOutputPath(jobs.get(2), finalOutputPath);

		for (Job job: jobs) {
			if (!job.waitForCompletion(true)) {
				System.exit(1);
			}
		}
	}
}
