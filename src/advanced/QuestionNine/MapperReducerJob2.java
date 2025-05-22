package advanced.QuestionNine;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MapperReducerJob2 {

    public static class TopPublisherMapper extends Mapper<LongWritable, Text, CountryGenreKey, CountryGenrePublisherValue> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            String country = parts[0];
            String genre = parts[1];
            String publisher = parts[2];
            double avg = Float.parseFloat(parts[3]);

            context.write(new CountryGenreKey(country, genre), new CountryGenrePublisherValue(publisher, avg, 0));

        }
    }


    public static class TopPublisherReducer extends Reducer<CountryGenreKey, CountryGenrePublisherValue, Text, Text> {
        @Override
        protected void reduce(CountryGenreKey key, Iterable<CountryGenrePublisherValue> values, Context context) throws IOException, InterruptedException {
            String topPublisher = null;
            double maxAvg = Double.MIN_VALUE;

            for (CountryGenrePublisherValue value : values) {
                double avg = value.getSales();
                if (avg > maxAvg) {
                    maxAvg = avg;
                    topPublisher = value.getPublisher();
                }
            }

            if (topPublisher != null) {
                String output = String.format("%s\t%s\t%.2f", key.getCountry(), key.getGenre(), maxAvg);
                context.write(new Text(topPublisher), new Text(output));
            }
        }
    }
}
