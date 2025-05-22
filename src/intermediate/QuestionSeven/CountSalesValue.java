package intermediate.QuestionSeven;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountSalesValue implements Writable {
    private double totalSalesPlatform;

    public CountSalesValue() {
    }

    public CountSalesValue(double totalSalesPlatform) {
        this.totalSalesPlatform = totalSalesPlatform;
    }

    public double getTotalSalesPlatform() {
        return totalSalesPlatform;
    }

    public void setTotalSalesPlatform(double totalSalesPlatform) {
        this.totalSalesPlatform = totalSalesPlatform;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(this.totalSalesPlatform);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.totalSalesPlatform = dataInput.readDouble();


    }

    @Override
    public String toString() {
        return String.valueOf(totalSalesPlatform);
    }
}
