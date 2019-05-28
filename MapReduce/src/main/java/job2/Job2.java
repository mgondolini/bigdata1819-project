package job2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

	public static class CategoryMapper extends Mapper<Object, Text, Text, Text> {
		private Text category = new Text();
		private Text tagList = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 11);
			if(tokens.length > 7){
				if(tokens[4].equals("category_id") || tokens[6].equals("tags")) {
					return;
				}else {
					category.set(tokens[4]);
					String[] tags = tokens[6].split("(\\|)");
					for (String tag : tags) {
						tagList.set(tag);
						context.write(category, tagList);
					}
				}
			}else return;
			}
	}

	public static class TagReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashMap<String, Integer> tagMap = new HashMap<>();

			for(Text tag: values) incrementTagOccurrence(tagMap, tag.toString());

			Map<String, Integer> orderedTags = sortByValue(tagMap);

			List<Map.Entry<String,Integer>> results = new ArrayList<>(orderedTags.entrySet());

			context.write(new Text("\ncategory: "+key), new Text("tags: "+ results.subList(0, 10)));
		}
	}

	private static <K, V> Map<K, V> sortByValue(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
		Collections.sort(list, new Comparator<Object>() {
			@SuppressWarnings("unchecked")
			public int compare(Object o1, Object o2) {
				return ((Comparable<V>) ((Map.Entry<K, V>) (o2)).getValue()).compareTo(((Map.Entry<K, V>) (o1)).getValue());
			}
		});
		Map<K, V> result = new LinkedHashMap<>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

	private static <K> void incrementTagOccurrence(Map<K,Integer> map, K key){
//		map.merge(key, 1, (a, b) -> a + b);

		Integer count = map.get(key);
		if(count == null){
			map.put(key, 1);
		}else{
			map.put(key, count+1);
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
