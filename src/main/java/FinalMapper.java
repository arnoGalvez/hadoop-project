import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.FileSystem.LOG;

public class FinalMapper extends Mapper<Object, Text, Cluster, Text> {
    private static int k;
    private static int col;// Coordinates starting columns
    private static int coordinatesCount;
    private static List<Point> oldcentroids = new ArrayList<Point>();

    public void setup (Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        k   = conf.getInt("k", -1);
        col = conf.getInt("col", -1);
        coordinatesCount = conf.getInt("coordinatesCount", 0);
        Path filename  = new Path(conf.get("centroids"));
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(filename));
        Cluster  key      = new Cluster(0);
        MeanData centroid = new MeanData( 1, new Point( 1 ));
        for (int i = 0; i < k; i++) {
            reader.next(key, centroid);
            oldcentroids.add(centroid.ComputeMean());
        }
        reader.close();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        List<Double> coords = new ArrayList<Double>();
        Point pt = null;
        try {
            coords.add(Double.parseDouble(tokens[col]));
            pt = new Point(coords);
        } catch (Exception e) {

            LOG.info( "FinalMapper: swallowing exception " + e.getMessage() );
        }
        if (pt != null) {
            int nearest = Point.getNearest(oldcentroids, pt);
            Cluster cluster = new Cluster(nearest);
            context.write(cluster, value);
        }
    }
}
