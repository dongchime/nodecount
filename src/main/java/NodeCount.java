import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class NodeCount extends Configured implements Tool {

    public long num_nodes;

    @Override
    public int run(String[] args) throws Exception {
        Job myjob = Job.getInstance(getConf());
        myjob.setJarByClass(NodeCount.class);
        myjob.setMapperClass(NCMapper.class);
        myjob.setReducerClass(NCReducer.class);
        myjob.setMapOutputKeyClass(LongWritable.class);
        myjob.setMapOutputValueClass(LongWritable.class);
        myjob.setOutputKeyClass(LongWritable.class);
        myjob.setOutputValueClass(LongWritable.class);
        myjob.setOutputFormatClass(TextOutputFormat.class);
        myjob.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(myjob, new Path(args[0]));
        FileOutputFormat.setOutputPath(myjob, new Path(args[1]));

        myjob.waitForCompletion(true);

        this.num_nodes = myjob.getCounters().findCounter(Counters.NUM_NODES).getValue();

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new NodeCount(), args);
//        ToolRunner.run(new NodeCount(), new String[]{"src/test/resources/simple.tsv", "src/test/resources/simple.out"});
    }

    public static class NCMapper extends Mapper<Object, Text, LongWritable, LongWritable> {
        LongWritable ok = new LongWritable();
        LongWritable ov = new LongWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());

            long u = Long.parseLong(st.nextToken());
            long v = Long.parseLong(st.nextToken());

            ok.set(u%10);
            ov.set(u);
            context.write(ok, ov);

            ok.set(v%10);
            ov.set(v);
            context.write(ok, ov);
        }
    }

    public static class NCReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        LongWritable counts = new LongWritable();
        LongWritable dummy = new LongWritable();
        long num_nodes;

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long parts = key.get();
            num_nodes = 0;

            LongOpenHashSet unique = new LongOpenHashSet();

            for (LongWritable val : values) {
                unique.add(val.get());
            }

            num_nodes += unique.size();
            counts.set(unique.size());
            dummy.set(-1);

            context.write(counts, dummy);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.getCounter(Counters.NUM_NODES).increment(num_nodes);
        }
    }




}
