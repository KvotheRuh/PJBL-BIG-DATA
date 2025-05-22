//1.Qual o jogo mais vendido globalmente (EASY)

package basic.QuestionOne;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class GlobalGameCount extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path("in/video_games_sales.csv");
        Path output = new Path("output/GlobalGameCount");

        Job job = Job.getInstance(conf, "Global-Game-Count");

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(MapperGameCount.class);
        job.setReducerClass(ReducerGameCount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;


    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(), new GlobalGameCount(), args);
        System.exit(result);
    }

    public static class MapperGameCount extends Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (line.startsWith("rank")) {
                return;
            }

            String[] columns = line.split(",");
            String gameName = columns[1];
            float globalSales = Float.parseFloat(columns[10]);
            context.write(new Text(gameName), new FloatWritable(globalSales));


        }
    }

    public static class ReducerGameCount extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        float sales = 0;
        Text game = new Text();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float totalSales = 0;
            for (FloatWritable v : values) {
                totalSales += v.get();
            }

            if (totalSales > sales) {
                sales = totalSales;
                game.set(key);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (game != null) {
                context.write(game, new FloatWritable(sales));
            }
        }
    }
}

