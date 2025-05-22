package advanced.QuestionNine;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CountryGenrePublisherKey implements WritableComparable<CountryGenrePublisherKey> {
    private String country;
    private String genre;

    public CountryGenrePublisherKey() {
    }

    public CountryGenrePublisherKey(String pais, String genero) {
        this.country = pais;
        this.genre = genero;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    @Override
    public int compareTo(CountryGenrePublisherKey o) {
        int c = country.compareTo(o.country);
        if (c != 0) {
            return c;
        }
        return genre.compareTo(o.genre);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        country = dataInput.readUTF();
        genre = dataInput.readUTF();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.country);
        dataOutput.writeUTF(this.genre);
    }


    @Override
    public String toString() {
        return country + "\t" + genre;
    }
}
