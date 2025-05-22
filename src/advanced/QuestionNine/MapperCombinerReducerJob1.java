package advanced.QuestionNine;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MapperCombinerReducerJob1 {

    public static class CountryGenrePublisherAvgMapper extends Mapper<LongWritable, Text, CountryGenrePublisherKey, CountryGenrePublisherValue> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("rank")) {
                return;
            }
            String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            String publisher = columns[5];
            String genre = columns[4];
            float naSales = Float.parseFloat(columns[6]);
            float euSales = Float.parseFloat(columns[7]);
            float jpSales = Float.parseFloat(columns[8]);
            float otherSales = Float.parseFloat(columns[9]);
            float globalSales = Float.parseFloat(columns[10]);

            context.write(new CountryGenrePublisherKey("NA", genre), new CountryGenrePublisherValue(publisher, naSales, 1));
            context.write(new CountryGenrePublisherKey("EU", genre), new CountryGenrePublisherValue(publisher, euSales, 1));
            context.write(new CountryGenrePublisherKey("JP", genre), new CountryGenrePublisherValue(publisher, jpSales, 1));
            context.write(new CountryGenrePublisherKey("Other", genre), new CountryGenrePublisherValue(publisher, otherSales, 1));
            context.write(new CountryGenrePublisherKey("Global", genre), new CountryGenrePublisherValue(publisher, globalSales, 1));
        }
    }

    public static class CountryGenrePublisherCombiner extends Reducer<CountryGenrePublisherKey, CountryGenrePublisherValue, CountryGenrePublisherKey, CountryGenrePublisherValue> {
        @Override
        protected void reduce(CountryGenrePublisherKey key, Iterable<CountryGenrePublisherValue> values,
                              Context context) throws IOException, InterruptedException {

            String currentPublisher = null;
            CountryGenrePublisherValue sum = new CountryGenrePublisherValue();

            for (CountryGenrePublisherValue value : values) {
                if (currentPublisher == null || !currentPublisher.equals(value.getPublisher())) {
                    if (currentPublisher != null) {
                        context.write(key, sum);
                    }
                    currentPublisher = value.getPublisher();
                    sum = new CountryGenrePublisherValue(currentPublisher, 0, 0);
                }
                sum.somar(value);
            }

            if (currentPublisher != null) {
                context.write(key, sum);
            }
        }
    }

    public static class CountryGenrePublisherAvgReducer extends Reducer<CountryGenrePublisherKey, CountryGenrePublisherValue, CountryGenrePublisherKey, Text>{
        @Override
        protected void reduce(CountryGenrePublisherKey key, Iterable<CountryGenrePublisherValue> values,
                              Context context) throws IOException, InterruptedException {
            String publisherAvg = null;
            double topMedia = Double.MIN_VALUE;

            for (CountryGenrePublisherValue value : values) {
                double mediaActual = value.getSales() / value.getCount();

                if (mediaActual > topMedia) {
                    topMedia = mediaActual;
                    publisherAvg = value.getPublisher();
                }
            }

            if (publisherAvg != null) {
                String resultFormat = String.format("%s\t%.2f", publisherAvg, topMedia);
                context.write(key, new Text(resultFormat));
            }
        }
    }
}
