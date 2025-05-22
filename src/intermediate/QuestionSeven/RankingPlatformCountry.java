//7.Obter ranking das plataformas por país (MEDIUM)

package intermediate.QuestionSeven;

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

public class RankingPlatformCountry extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path("in/video_games_sales.csv");
        Path output = new Path("output/RankingPlatformCountry");

        Job job = Job.getInstance(conf, "Ranking-Platform-Country");

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(RankingPlatformMapper.class);
        job.setReducerClass(RankingPlatformReducer.class);
        job.setCombinerClass(RankingPlatformCombiner.class);


        job.setMapOutputKeyClass(PlatformCountryKey.class);
        job.setMapOutputValueClass(CountSalesValue.class);

        job.setOutputKeyClass(PlatformCountryKey.class);
        job.setOutputValueClass(FloatWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;


    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(), new RankingPlatformCountry(), args);
        System.exit(result);
    }

    public static class RankingPlatformMapper extends Mapper<LongWritable, Text, PlatformCountryKey, CountSalesValue> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("rank")) {
                return;
            }

            String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            String platform = columns[2];
            float naSales = Float.parseFloat(columns[6]);
            float euSales = Float.parseFloat(columns[7]);
            float jpSales = Float.parseFloat(columns[8]);
            float otherSales = Float.parseFloat(columns[9]);
            float globalSales = Float.parseFloat(columns[10]);

            context.write(new PlatformCountryKey(platform, "NA"), new CountSalesValue(naSales));
            context.write(new PlatformCountryKey(platform, "EU"), new CountSalesValue(euSales));
            context.write(new PlatformCountryKey(platform, "JP"), new CountSalesValue(jpSales));
            context.write(new PlatformCountryKey(platform, "OTHER"), new CountSalesValue(otherSales));
            context.write(new PlatformCountryKey(platform, "GLOBAL"), new CountSalesValue(globalSales));

        }
    }

    public static class RankingPlatformCombiner extends Reducer<PlatformCountryKey, CountSalesValue, PlatformCountryKey, CountSalesValue> {
        public void reduce(PlatformCountryKey key, Iterable<CountSalesValue> values, Context context) throws IOException, InterruptedException {

            double total = 0;

            for (CountSalesValue v : values) {
                total += v.getTotalSalesPlatform();
            }

            context.write(key, new CountSalesValue(total));
        }
    }

    public static class RankingPlatformReducer extends Reducer<PlatformCountryKey, CountSalesValue, Text, Text> {
        private Map<String, Map<String, Double>> countryPlatformMap = new HashMap<>();

        public void reduce(PlatformCountryKey key, Iterable<CountSalesValue> values, Context context) throws IOException, InterruptedException {
            double totalSales = 0;

            for (CountSalesValue v : values) {
                totalSales += v.getTotalSalesPlatform();
            }

            String country = key.getCountry();
            String platform = key.getPlatform();

            Map<String, Double> platformSales = countryPlatformMap.computeIfAbsent(country, k -> new HashMap<>());

            platformSales.put(platform, platformSales.getOrDefault(platform, 0.0) + totalSales);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Map<String, Double>> entry : countryPlatformMap.entrySet()) {
                String country = entry.getKey();
                Map<String, Double> platformSales = entry.getValue();

                List<Map.Entry<String, Double>> orderingPlatform = new ArrayList<>(platformSales.entrySet());

                orderingPlatform.sort((e1, e2) -> Double.compare(e2.getValue(), e1.getValue()));

                for (Map.Entry<String, Double> e : orderingPlatform) {
                    String textStringCountry = String.format("%-10s", country);
                    String textStringKey = String.format("%-10s", e.getKey());
                    String textStringValue = String.format("%-10s", e.getValue());
                    context.write(new Text(textStringCountry), new Text(textStringKey + textStringValue));
                }
            }
        }

    }
}