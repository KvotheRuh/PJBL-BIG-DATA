package intermediate.QuestionFive;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GenrePlataformKey implements WritableComparable<GenrePlataformKey> {
    private String genre;
    private String platform;

    public GenrePlataformKey() {
    }

    public GenrePlataformKey(String genre, String plataform) {
        this.genre = genre;
        this.platform = plataform;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public int compareTo(GenrePlataformKey key){
        int r = this.genre.compareTo(key.getGenre());
        if( r != 0 ){
            return r;
        }
        return this.platform.compareTo(key.getPlatform());
    }

    public void readFields(DataInput dataInput) throws IOException{
        this.genre = dataInput.readUTF();
        this.platform = dataInput.readUTF();
    }

    public String toString(){
        return String.format("Genre: %-15s Platform: %-10s Average: ", genre, platform);
    }

    public void write(DataOutput dataOutput) throws IOException{
        dataOutput.writeUTF(this.genre);
        dataOutput.writeUTF(this.platform);
    }
}
