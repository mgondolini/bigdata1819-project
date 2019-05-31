package job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class Job1 {

	private final static String GOOD = "good";
	private final static String BAD = "bad";
	private final static String NEUTRAL = "neutral";
	private final static int THRESHOLD_MAX = 6;
	private final static int THRESHOLD_MIN = 4;

	public static class CommentsMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text classification = new Text();
		private IntWritable commentsIndex = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 14);

			if(tokens[0].equals("video_id") || tokens[12].equals("True") || tokens[13].equals("True")) {
				return;
			} else {
				int likes = Integer.valueOf(tokens[8]);
				int dislikes = Integer.valueOf(tokens[9]);
				int comments = Integer.valueOf(tokens[10]);
				int views = Integer.valueOf(tokens[7]);

				if(comments == 0) comments = 1;

				commentsIndex.set(comments);

				if(dislikes == 0) dislikes = 1;

				if((likes/(dislikes)) > THRESHOLD_MAX){
					classification.set(GOOD);
				}else if((likes/(dislikes)) < THRESHOLD_MIN){
					classification.set(BAD);
				}else{
					classification.set(NEUTRAL);
				}
			}
			context.write(classification, commentsIndex);
		}
	}

	public static class CommentsReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			double tot = 0, count = 0;
			for (IntWritable value : values) {
				count++;
				tot += value.get();
			}
			context.write(new Text(key+" count:"+count), new DoubleWritable(tot / count));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job1");

		job.setJarByClass(Job1.class);
		job.setMapperClass(CommentsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(CommentsReducer.class);

		FileSystem fs = FileSystem.get(new Configuration());
		Path outputPath = new Path(args[0]);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CommentsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, CommentsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, CommentsMapper.class);

		//MI 4 con categorie json

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
