import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.SequenceFile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.print.DocFlavor;


class KmeansMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Cluster, MeanData> {
    private static int k;
    private static int colCount;// Coordinates starting columns
    private static int coordinatesCount;
    private static List<Integer> cols = new ArrayList<Integer>();
    private static List<Point> oldcentroids = new ArrayList<Point>();
    private static final Log LOG = LogFactory.getLog(KmeansMapper.class);


    public void setup (Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        k   = conf.getInt("k", -1);
        colCount = conf.getInt("colCount", -1);
        for (int i = 0; i < colCount; i++) {
            cols.add(conf.getInt("col"+i, -1));
        }
        coordinatesCount = conf.getInt("coordinatesCount", 0);
        Path   filename  = new Path(conf.get("centroids"));
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(filename));
        Cluster  key      = new Cluster(0);
        MeanData centroid = new MeanData(1, new Point( colCount ));
        for (int i = 0; i < k; i++) {
            reader.next(key, centroid);
            oldcentroids.add(centroid.ComputeMean());
        }
        // throw new IOException("Pts " + oldcentroids);
        // reader.close();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] tokens = value.toString().split( "," );
        List<Double> coords = new ArrayList<Double>();
        try {
            for(Integer col : cols) {
                coords.add(Double.parseDouble(tokens[col]));
            }
        } catch ( Exception e )
        {
            LOG.info( "KmeansMapper: swallowing exception " + e.getMessage() );
        }
        if (coords.size()!=0) {
            Point pt = new Point(coords);

            int nearest = Point.getNearest(oldcentroids, pt);
            MeanData centroid = new MeanData(1, pt);
            Cluster c = new Cluster(nearest);
            context.write(c, centroid);
        }
    }
}
