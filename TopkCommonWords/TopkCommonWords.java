// TopkCommonWords
// test
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Comparator;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {

    public static class WordInputMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private String filename = new String();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            FileSplit filesplit = ((FileSplit) context.getInputSplit());
            filename = filesplit.getPath().getName();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("[\\s\\t\\n\\r\\f]");
            for (String token : tokens) {
                if (token.equals("")) {
                    continue;
                }
                word.set(token);
                context.write(word, new Text(filename));
            }
        }
    }

    public static class WordSumReducer
            extends Reducer<Text, Text, IntWritable, Text> {

        int result = 0;
        private HashMap<String, Integer> wordCountMap = new HashMap<String, Integer>();

        @Override
        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            int doc1Sum = 0;
            int doc2Sum = 0;
            for (Text valueText : values) {
                String value = valueText.toString();
                if (value.equals("task1-input1.txt")) {
                    doc1Sum++;
                } else if (value.equals("task1-input2.txt")) {
                    doc2Sum++;
                } else {
                    return;
                }
            }
            result = Math.min(doc1Sum, doc2Sum);
            wordCountMap.put(key.toString(), result);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<>();
            wordCountMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
            ArrayList<String> keys = new ArrayList<String>(sortedMap.keySet());
            for (int i = 0; i < 20; i++) {
                String key = keys.get(i);
                Text word = new Text(key);
                IntWritable count = new IntWritable(sortedMap.get(key));
                context.write(count, word);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "commonwords");
        job.setJarByClass(TopkCommonWords.class);
        job.setMapperClass(WordInputMapper.class);
        job.setReducerClass(WordSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
