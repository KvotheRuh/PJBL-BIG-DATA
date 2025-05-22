//3.A quantidade de jogos que cada gênero possui (EASY)

package basic.QuestionTwo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class GenreGameAmount extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path("in/video_games_sales.csv");
        Path output = new Path("output/GenreGameAmount");

        Job job = Job.getInstance(conf, "Genre-Game-Amount");

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(MapperGenreGameAmount.class);
        job.setReducerClass(ReducerGenreGameAmount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;


    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(),new GenreGameAmount(), args);
        System.exit(result);
    }

    public static class MapperGenreGameAmount extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (line.startsWith("rank")) {
                return;
            }

            String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            String gameGenre = columns[4];
            context.write(new Text(gameGenre), new IntWritable(1));
        }
    }

    public  static class ReducerGenreGameAmount extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable i: values){
                count += i.get();
            }

            String formattedKey = String.format("%-15s", key.toString());
            context.write(new Text(formattedKey), new IntWritable(count));

        }
    }
}
