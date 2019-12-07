public class MeanData<Point extends PlusDivide> {
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
}


