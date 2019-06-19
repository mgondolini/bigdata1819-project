package prejob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class TrendingVideosUnion {

    public static class UnionMapper extends Mapper<Object, Text, Text, Text> {
        private Text newKey = new Text();
        private Text newLine = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String line = value.toString();
//            StringTokenizer tokenizer = new StringTokenizer(line, ",");
//            int i = 0;
//            while(tokenizer.hasMoreTokens() && i == 0){
//                newKey.set(tokenizer.nextToken());
//                i++;
//            }
//            newLine.set(line);
            String line = value.toString();
            String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 14);

            if(tokens[0].equals("video_id") || tokens[12].equals("True") || tokens[13].equals("True")) {
                return;
            } else {
                int dislikes = Integer.valueOf(tokens[9]);
                int comments = Integer.valueOf(tokens[10]);

                if (comments == 0) comments = 1;
                if (dislikes == 0) dislikes = 1;

                context.write(new Text(key.toString()), value);
            }
        }
    }

    public static class UnionReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "trending videos union");

        job.setJarByClass(TrendingVideosUnion.class);
        job.setMapperClass(UnionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(UnionReducer.class);

        FileSystem fs = FileSystem.get(new Configuration());
        Path outputPath = new Path(args[0]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UnionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, UnionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, UnionMapper.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
