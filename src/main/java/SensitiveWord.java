import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SensitiveWord extends ToolRunner implements Tool {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        static List<String> li = new ArrayList<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path p : paths) {
                String fileName = p.getName();
                if (fileName.equals("dir")) {
                    BufferedReader sb = null;
                    sb = new BufferedReader(new FileReader(new File(p.toString())));
                    String tmp = null;
                    while ((tmp = sb.readLine()) != null) {
                        String ss[] = tmp.split(" ");
                        for (String s : ss) {
                            li.add(s);
                        }
                    }
                    sb.close();
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer lines = new StringTokenizer(line);
            while (lines.hasMoreTokens()) {
                String word = lines.nextToken();
                if (!li.contains(word)) {
                    context.write(new Text(word), new Text("1"));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        }

    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            int counter = 0;
            for (Text t : value) {
                counter += Integer.parseInt(t.toString());
            }
            context.write(key, new Text(counter + ""));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        }
    }


    @Override
    public void setConf(Configuration conf) {
        conf.set("fs.defaultFS", "hdfs://master:9000");
    }

    @Override
    public Configuration getConf() {
        return new Configuration();
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "SensitiveWord");
        job.setJarByClass(SensitiveWord.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.addCacheFile(new URI("hdfs://master:9000/dir"));
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int isok = job.waitForCompletion(true) ? 0 : 1;
        return isok;
    }

    public static void main(String[] args) {
        try {
            String[] argss = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();
            System.exit(ToolRunner.run(new SensitiveWord(), argss));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}