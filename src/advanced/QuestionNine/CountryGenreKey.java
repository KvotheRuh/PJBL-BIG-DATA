package advanced.QuestionNine;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountryGenreKey implements WritableComparable<CountryGenreKey> {
    private String country;
    private String genre;

    public CountryGenreKey() {
    }

    public CountryGenreKey(String country, String genre) {
        this.country = country;
        this.genre = genre;
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
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(country);
        dataOutput.writeUTF(genre);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        country = dataInput.readUTF();
        genre = dataInput.readUTF();
    }

    @Override
    public int compareTo(CountryGenreKey o) {
        int c = country.compareTo(o.country);
        if (c != 0) return c;
        return genre.compareTo(o.genre);
    }


    @Override
    public String toString() {
        return country + "\t" + genre;
    }
}

