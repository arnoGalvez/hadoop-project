import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MeanData<Point extends PlusDivide> implements Writable {
    int count;
    Point sum;

    public MeanData(int count, Point sum)
    {
        this.count = count;
        this.sum = sum;
    }

    public MeanData<Point> Combine(MeanData<Point> rhs)
    {
        return new MeanData<Point>( count + rhs.count, (Point)sum.Add( rhs.sum ));
    }

    public Point ComputeMean()
    {
        return (Point)sum.Divide( count );
    }


    public void readFields(DataInput in) throws IOException { sum = in.readInt(); count = in.readInt(); }
    public void write(DataOutput out) throws IOException { out.writeInt(sum); out.writeInt(count); }

}


