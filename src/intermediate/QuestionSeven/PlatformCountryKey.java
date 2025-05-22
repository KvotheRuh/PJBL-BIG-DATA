package intermediate.QuestionSeven;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PlatformCountryKey implements WritableComparable<PlatformCountryKey> {
    private String platform;
    private String country;

    public PlatformCountryKey() {
    }

    public PlatformCountryKey(String platform, String country) {
        this.platform = platform;
        this.country = country;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public int compareTo(PlatformCountryKey o) {
        int p = this.platform.compareTo(o.platform);
        if (p != 0){
            return p;
        }
        return this.country.compareTo(o.getCountry());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.platform = dataInput.readUTF();
        this.country = dataInput.readUTF();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.platform);
        dataOutput.writeUTF(this.country);
    }
}

