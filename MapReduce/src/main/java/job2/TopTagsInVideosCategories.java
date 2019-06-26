package job2;

import com.cloudera.org.codehaus.jackson.JsonFactory;
import com.cloudera.org.codehaus.jackson.JsonParser;
import com.cloudera.org.codehaus.jackson.JsonToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TopTagsInVideosCategories {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        ArrayList<Job> jobs = new ArrayList<>();
        jobs.add(Job.getInstance(conf, "Category:Tag | Total"));
        jobs.add(Job.getInstance(conf, "Category | [Tag:Total]"));
        jobs.add(Job.getInstance(conf, "Category Name | [Tag:Total]"));

        for (Job job: jobs){
            job.setJarByClass(TopTagsInVideosCategories.class);
        }

        MultipleInputs.addInputPath(jobs.get(0), new Path(args[0]), TextInputFormat.class, TopTagsInVideosCategories.CategoryTagOccurenceMapper.class);
        MultipleInputs.addInputPath(jobs.get(0), new Path(args[1]), TextInputFormat.class, TopTagsInVideosCategories.CategoryTagOccurenceMapper.class);
        MultipleInputs.addInputPath(jobs.get(0), new Path(args[2]), TextInputFormat.class, TopTagsInVideosCategories.CategoryTagOccurenceMapper.class);

        jobs.get(0).setMapOutputKeyClass(Text.class);
        jobs.get(0).setMapOutputValueClass(IntWritable.class);
        jobs.get(0).setOutputFormatClass(TextOutputFormat.class);
        jobs.get(0).setCombinerClass(TopTagsInVideosCategories.CategoryTagTotalReducer.class);
        jobs.get(0).setReducerClass(TopTagsInVideosCategories.CategoryTagTotalReducer.class);

        FileSystem fs = FileSystem.get(new Configuration());
        Path firstJobOutputPath = new Path(args[3]);
        if (fs.exists(firstJobOutputPath)) {
            fs.delete(firstJobOutputPath, true);
        }
        FileOutputFormat.setOutputPath(jobs.get(0), firstJobOutputPath);

        FileInputFormat.addInputPath(jobs.get(1), firstJobOutputPath);

        jobs.get(1).setInputFormatClass(KeyValueTextInputFormat.class);
        jobs.get(1).setMapperClass(AddTotalToCategoryKeyMapper.class);
        jobs.get(1).setReducerClass(CategoryTopTagReducer.class);

        jobs.get(1).setPartitionerClass(CategoryPartitioner.class);
        jobs.get(1).setSortComparatorClass(SortCategoryTotalComparator.class);
        jobs.get(1).setGroupingComparatorClass(CategoryReduceGrouping.class);

        jobs.get(1).setMapOutputKeyClass(Text.class);
        jobs.get(1).setMapOutputValueClass(Text.class);

        Path secondOutputPath = new Path(args[4]);
        if (fs.exists(secondOutputPath)) {
            fs.delete(secondOutputPath, true);
        }
        FileOutputFormat.setOutputPath(jobs.get(1), secondOutputPath);

        MultipleInputs.addInputPath(jobs.get(2), secondOutputPath, KeyValueTextInputFormat.class, DummyMapper.class);
        MultipleInputs.addInputPath(jobs.get(2), new Path(args[6]), TextInputFormat.class, CategoryNamingMapper.class);

        jobs.get(2).setReducerClass(CategoryNamingReducer.class);
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

    private final static String SPLIT_OPERATOR = "#";

    // JOB1
    private final static String SPLIT_REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private final static int FIELDS_NUMBER = 11;

    public static class CategoryTagOccurenceMapper extends Mapper<Object, Text, Text, IntWritable> {

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
                    categoryTagKey.set(tokens[4].concat(SPLIT_OPERATOR + tag.toLowerCase()));
                    context.write(categoryTagKey, one);
                }
            }
        }
    }

    public static class CategoryTagTotalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    // JOB2
    public static class AddTotalToCategoryKeyMapper extends Mapper<Text, Text, Text, Text> {

        private Text category = new Text();
        private Text tagCount = new Text();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = key.toString().split(SPLIT_OPERATOR, 2);

            category.set(tokens[0] + SPLIT_OPERATOR + value.toString());

            tagCount.set(tokens[1] + SPLIT_OPERATOR + value.toString());

            context.write(category, tagCount);
        }
    }

    /**
     * Define how map output will be sorted
     */
    public static class SortCategoryTotalComparator extends WritableComparator {

        protected SortCategoryTotalComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {

            String[] keyATokens = a.toString().split(SPLIT_OPERATOR, 2);
            String[] keyBTokens = b.toString().split(SPLIT_OPERATOR, 2);

            String keyA = keyATokens[0];
            String keyB = keyBTokens[0];
            Integer totalA = Integer.parseInt(keyATokens[1]);
            Integer totalB = Integer.parseInt(keyBTokens[1]);

            if (keyA.equals(keyB)) {
                return totalB.compareTo(totalA);
            } else {
                return keyA.compareTo(keyB);
            }
        }
    }

    /**
     * Define how lines will be send to reducer
     */
    public static class CategoryPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text text, Text text2, int numberOfPartitions) {
            String[] keyTokens = text.toString().split(SPLIT_OPERATOR, 2);
            return Math.abs(keyTokens[0].hashCode() % numberOfPartitions);
        }
    }

    /**
     * Define how lines will be grouped in reduce method
     */
    public static class CategoryReduceGrouping extends WritableComparator {

        protected CategoryReduceGrouping() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String[] keyATokens = a.toString().split(SPLIT_OPERATOR, 2);
            String[] keyBTokens = b.toString().split(SPLIT_OPERATOR, 2);

            return keyATokens[0].compareTo(keyBTokens[0]);
        }
    }

    public static class CategoryTopTagReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyTokens = key.toString().split(SPLIT_OPERATOR, 2);

            int n = 0;

            List<String> top10Tags = new ArrayList<>();

            Iterator<Text> iterator = values.iterator();

            while(n < 10 && iterator.hasNext()) {
                top10Tags.add(iterator.next().toString());
                n++;
            }

            context.write(new Text(keyTokens[0]), new Text(top10Tags.toString()));
        }

    }

    //JOB3
    public static class CategoryNamingMapper extends Mapper<Object, Text, Text, Text> {

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
                        nameCategoryString.set("join-category" + SPLIT_OPERATOR + parser.getText());
                    }
                }
            }
            context.write(idCategoryString, nameCategoryString);
        }
    }

    public static class DummyMapper extends Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class CategoryNamingReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String categoryName = "";
            String categoryTags = "";

            for(Text val : values) {
                if (val.toString().contains("join-category")) {
                    categoryName = val.toString().split(SPLIT_OPERATOR, 2)[1];
                } else {
                    categoryTags = val.toString();
                }
            }

            if(!categoryTags.equals("")) {
                context.write(new Text(categoryName), new Text(categoryTags));
            }
        }
    }
}
