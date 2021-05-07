import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class NodeCount extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job myjob = Job.getInstance(getConf());
        myjob.setJarByClass(NodeCount.class);
        myjob.setMapperClass(NCMapper.class);
        myjob.setMapOutputKeyClass(LongWritable.class);
        myjob.setMapOutputValueClass(IntWritable.class);
        myjob.setOutputFormatClass(TextOutputFormat.class);
        myjob.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(myjob, new Path(args[0]));
        FileOutputFormat.setOutputPath(myjob, new Path(args[0]).suffix(".out"));

        myjob.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new NodeCount(), new String[]{"src/test/resources/simple.tsv"});
    }

    public static class NCMapper extends Mapper<Object, Text, LongWritable, IntWritable> {
//        int max = 0;
        LongOpenHashSet unique;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            unique = new LongOpenHashSet();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());

            long u = Long.parseLong(st.nextToken());
            long v = Long.parseLong(st.nextToken());

            if (!unique.contains(u)) unique.add(u);
            if (!unique.contains(v)) unique.add(v);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(unique.size()), new IntWritable(0));
        }
    }
}
