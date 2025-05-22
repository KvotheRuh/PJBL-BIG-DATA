package intermediate.QuestionFive;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GenrePlataformValue implements Writable {
    private float sales;
    private int count;

    public GenrePlataformValue(float sales, int count) {
        this.sales = sales;
        this.count = count;
    }

    public GenrePlataformValue() {
    }

    public float getSales() {
        return sales;
    }

    public void setSales(float sales) {
        this.sales = sales;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sales = dataInput.readFloat();
        this.count = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(this.sales);
        dataOutput.writeInt(this.count);
    }
}
