package advanced.QuestionNine;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountryGenrePublisherValue implements Writable {
    private String publisher;
    private double sales;
    private int count;

    public CountryGenrePublisherValue() {
    }

    public CountryGenrePublisherValue(String publisher, double sales, int count) {
        this.publisher = publisher;
        this.count = count;
        this.sales = sales;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public double getSales() {
        return sales;
    }

    public void setSales(double sales) {
        this.sales = sales;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void somar(CountryGenrePublisherValue other) {
        this.sales += other.sales;
        this.count += other.count;
    }

    public double getMedia() {
        return count == 0 ? 0.0 : sales / count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(publisher);
        dataOutput.writeDouble(sales);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        publisher = dataInput.readUTF();
        sales = dataInput.readDouble();
        count = dataInput.readInt();
    }

    @Override
    public String toString() {
        return publisher + "\t" + String.format("%.2f", getMedia());
    }
}
