//9.Qual o publisher com maior média de venda por país/gênero (HARD)

package advanced.QuestionNine;

import intermediate.QuestionSeven.PlatformCountryKey;
import intermediate.QuestionSeven.RankingPlatformCountry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class PublisherGenreCountryAvg extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path("in/video_games_sales.csv");
        Path output = new Path("output/PublisherGenreCountryAvg");
        Path inputJob2 = new Path("output/PublisherGenreCountryAvg.txt");

        Job job1 = Job.getInstance(conf, "Publisher-Genre-Country-Avg");

        FileInputFormat.addInputPath(job1, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job1, output);

        job1.setMapperClass(MapperCombinerReducerJob1.CountryGenrePublisherAvgMapper.class);
        job1.setReducerClass(MapperCombinerReducerJob1.CountryGenrePublisherAvgReducer.class);
        job1.setCombinerClass(MapperCombinerReducerJob1.CountryGenrePublisherCombiner.class);

        job1.setMapOutputKeyClass(CountryGenrePublisherKey.class);
        job1.setMapOutputValueClass(CountryGenrePublisherValue.class);

        job1.setOutputKeyClass(CountryGenrePublisherKey.class);
        job1.setOutputValueClass(CountryGenrePublisherValue.class);




        boolean job1Success = job1.waitForCompletion(true);

        if (!job1Success) {
            System.out.println("Failed to execute Job 1");
            return 1;
        }

        Job job2 = Job.getInstance(conf, "Publisher-Avg");

        FileInputFormat.addInputPath(job2, inputJob2);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job2, output);

        job2.setMapperClass(MapperReducerJob2.TopPublisherMapper.class);
        job2.setReducerClass(MapperReducerJob2.TopPublisherReducer.class);


        job2.setMapOutputKeyClass(CountryGenrePublisherKey.class);
        job2.setMapOutputValueClass(CountryGenrePublisherValue.class);

        job2.setOutputKeyClass(CountryGenrePublisherKey.class);
        job2.setOutputValueClass(CountryGenrePublisherValue.class);

        return job2.waitForCompletion(true) ? 0 : 1;


    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(), new PublisherGenreCountryAvg(), args);
        System.exit(result);
    }


}
