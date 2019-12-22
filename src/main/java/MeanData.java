import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class MeanData implements Writable {
    private int pointsCount;
    private int coordsCount;
    private Point sum;

    public MeanData()
    {
        pointsCount = 0;
        coordsCount = 0;
        Point sum = null;
    }

    public MeanData(MeanData meanData)
    {
        pointsCount = meanData.pointsCount;
        coordsCount = meanData.coordsCount;
        sum = meanData.sum;
    }

    @Override
    public String toString()
    {
        return "Points : " + pointsCount + " - Mean : " + sum;
    }

    public MeanData(int pointsCount, Point sum)
    {
        this.pointsCount = pointsCount;
        this.sum = sum;
        coordsCount = sum.size();
    }

    public static MeanData Combine(MeanData lhs, MeanData rhs)
    {
        Point weightedSum =  Point.scale( Point.add(Point.scale(lhs.sum, lhs.pointsCount), Point.scale(rhs.sum, rhs.pointsCount)), 1.0 / (lhs.pointsCount + rhs.pointsCount));
        return new MeanData( lhs.pointsCount + rhs.pointsCount, weightedSum);
    }

    public Point ComputeMean()
    {
        return sum;
    }


    public void readFields(DataInput in) throws IOException {
        pointsCount = in.readInt();
        coordsCount = in.readInt();
        List<Double> coords = new ArrayList<Double>(coordsCount);
        for (int i = 0; i < coordsCount; ++i)
        {
            coords.add(in.readDouble());
        }
        sum = new Point(coords);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(pointsCount);
        out.writeInt(coordsCount);
        List<Double> coords = sum.getCoords();
        for (Double scalar : coords)
        {
            out.writeDouble(scalar);
        }
    }

}


