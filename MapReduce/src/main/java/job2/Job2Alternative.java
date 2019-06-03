package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Job2Alternative {

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

            String[] tokens = key.toString().split(":", 2);

            category.set(tokens[0] + ":" + value.toString());

            tagCount.set(tokens[0] + ":" + tokens[1] + ":" + value.toString());

            context.write(category, tagCount);
        }
    }

    public static class TagReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] tokens = key.toString().split(":", 2);

            context.write(new Text(tokens[0]), values.iterator().next());
        }
    }

    public static class GroupByCategoryMapper extends Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class GroupByCategoryReducer extends Reducer<Text, Text, Text, List<Text>> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Text> tagWithMostOccurence = new ArrayList<>();

            int i = 0;
            for(Text value : values) {
                if(i == 20) return;

                tagWithMostOccurence.add(value);

                i++;
            }

            context.write(new Text(key), tagWithMostOccurence);

        }
    }

    public static class TagCustomComparator extends WritableComparator {

        protected TagCustomComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text firstKey = (Text) a;
            Text secondKey = (Text) b;

            String[] firstKeyTokens = firstKey.toString().split(":", 3);
            String[] secondKeyTokens = secondKey.toString().split(":", 3);

            Integer firstOccurences = Integer.valueOf(firstKeyTokens[1]);
            Integer secondOccurences = Integer.valueOf(secondKeyTokens[1]);
            String firstCategory = firstKeyTokens[0];
            String secondCategory = secondKeyTokens[0];

            int stringOrder = firstCategory.compareTo(secondCategory);

            if(stringOrder == 0) {
                return -1 * firstOccurences.compareTo(secondOccurences);
            } else if(stringOrder > 0) {
                return 1;

            } else {
                return -1;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        ArrayList<Job> jobs = new ArrayList<>();
        jobs.add(Job.getInstance(conf, "First Job"));
        jobs.add(Job.getInstance(conf, "Second Job"));
        jobs.add(Job.getInstance(conf, "Third Job"));

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

        // JOB 2

        FileInputFormat.addInputPath(jobs.get(1), firstJobOutputPath);
        Path secondJobOutputPath = new Path(args[4]);
        if (fs.exists(secondJobOutputPath)) {
            fs.delete(secondJobOutputPath, true);
        }
        FileOutputFormat.setOutputPath(jobs.get(1), secondJobOutputPath);
        jobs.get(1).setInputFormatClass(KeyValueTextInputFormat.class);
        jobs.get(1).setMapperClass(Job2Alternative.TagMapper.class);
        jobs.get(1).setMapOutputKeyClass(Text.class);
        jobs.get(1).setMapOutputValueClass(Text.class);
        jobs.get(1).setReducerClass(Job2Alternative.TagReducer.class);
        jobs.get(1).setSortComparatorClass(Job2Alternative.TagCustomComparator.class);
        jobs.get(1).setOutputFormatClass(TextOutputFormat.class);

        // JOB 3

        FileInputFormat.addInputPath(jobs.get(2), secondJobOutputPath);
        Path finalJobOutputPath = new Path(args[5]);
        if (fs.exists(finalJobOutputPath)) {
            fs.delete(finalJobOutputPath, true);
        }
        FileOutputFormat.setOutputPath(jobs.get(2), finalJobOutputPath);
        jobs.get(2).setInputFormatClass(KeyValueTextInputFormat.class);
        jobs.get(2).setMapperClass(Job2Alternative.GroupByCategoryMapper.class);
        jobs.get(2).setMapOutputKeyClass(Text.class);
        jobs.get(2).setMapOutputValueClass(Text.class);
        jobs.get(2).setReducerClass(Job2Alternative.GroupByCategoryReducer.class);
        jobs.get(2).setOutputFormatClass(TextOutputFormat.class);

        for (Job job: jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }

    }
}
