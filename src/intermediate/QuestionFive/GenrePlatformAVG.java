
//5.Gênero e plataforma com maior média de vendas pela Europa (MEDIUM)


package intermediate.QuestionFive;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenrePlatformAVG extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path("in/video_games_sales.csv");
        Path output = new Path("output/GenrePlatformAVG");

        Job job = Job.getInstance(conf, "Genre-Platform-AVG");

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(AvgSalesMapper.class);
        job.setReducerClass(AvgSalesReducer.class);
        job.setCombinerClass(AvgSalesCombiner.class);


        job.setMapOutputKeyClass(GenrePlataformKey.class);
        job.setMapOutputValueClass(GenrePlataformValue.class);

        job.setOutputKeyClass(GenrePlataformKey.class);
        job.setOutputValueClass(FloatWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;


    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(), new GenrePlatformAVG(), args);
        System.exit(result);
    }

    public static class AvgSalesMapper extends Mapper<LongWritable, Text, GenrePlataformKey, GenrePlataformValue> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (line.startsWith("rank")) {
                return;
            }

            String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            String genre = columns[4];
            String plataform = columns[2];
            String salesEU = columns[7];


            if (!genre.isEmpty() && !plataform.isEmpty() && !salesEU.isEmpty()) {
                float sale = Float.parseFloat(salesEU);

                GenrePlataformKey k = new GenrePlataformKey(genre, plataform);
                GenrePlataformValue v = new GenrePlataformValue(sale, 1);

                context.write(k, v);
            }

        }


    }

    public static class AvgSalesCombiner extends Reducer<GenrePlataformKey, GenrePlataformValue, GenrePlataformKey, GenrePlataformValue> {
        @Override
        public void reduce(GenrePlataformKey key, Iterable<GenrePlataformValue> values, Context context) throws IOException, InterruptedException {
            float sumA = 0;
            int sumB = 0;

            for (GenrePlataformValue v : values) {
                sumA += v.getSales();
                sumB += v.getCount();
            }

            GenrePlataformValue value = new GenrePlataformValue(sumA, sumB);
            context.write(key, value);
        }
    }

    public static class AvgSalesReducer extends Reducer<GenrePlataformKey, GenrePlataformValue, GenrePlataformKey, FloatWritable> {

        private final Map<GenrePlataformKey, Float> avgMap = new HashMap<>();

        @Override
        protected void reduce(GenrePlataformKey key, Iterable<GenrePlataformValue> values, Context context) {
            float totalSales = 0;
            int totalCount = 0;

            for (GenrePlataformValue value : values) {
                totalSales += value.getSales();
                totalCount += value.getCount();
            }

            if (totalCount > 0) {
                float avg = totalSales / totalCount;
                avgMap.put(new GenrePlataformKey(key.getGenre(), key.getPlatform()), avg);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<GenrePlataformKey, Float>> sortedList = new ArrayList<>(avgMap.entrySet());
            sortedList.sort((e1, e2) -> Float.compare(e2.getValue(), e1.getValue()));

            for (Map.Entry<GenrePlataformKey, Float> entry : sortedList) {
                context.write(entry.getKey(), new FloatWritable(entry.getValue()));
            }
        }
    }


}
