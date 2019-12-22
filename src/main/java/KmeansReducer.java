import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

import java.util.HashMap;
import java.util.Iterator;

public class KmeansReducer extends Reducer<Cluster, MeanData, Cluster, MeanData> {

    HashMap<Cluster, MeanData> newCentroids = new HashMap<Cluster, MeanData>();

    static final String ConfStringHasConverged = "HasConverged";

    boolean HasConverged(HashMap<Cluster, MeanData> oldCentroids)
    {
        final double eps = 0.1;
        Iterator<Cluster> clusterIterator = newCentroids.keySet().iterator();
        while (clusterIterator.hasNext())
        {
            Cluster cluster = clusterIterator.next();
            Point point1 = newCentroids.get( cluster ).ComputeMean();
            Point point2 = oldCentroids.get( cluster ).ComputeMean();
            Point vec = Point.sub( point1, point2 );
            double sqrDist = vec.norm();
            if (sqrDist > eps)
            {
                return false;
            }

        }
        return true;
    }

    @Override
    public void reduce(Cluster cluster,
                       Iterable<MeanData> means,
                       Context context) throws IOException, InterruptedException
    {
        MeanData result = means.iterator().next();
        while(means.iterator().hasNext())
        {
            result = MeanData.Combine( result, means.iterator().next() );
        }
        newCentroids.put( cluster, result );
        context.write( cluster, result );
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get( conf );
        Path centersPath = new Path(conf.get("centersFilePath"));


        SequenceFile.Reader centerReader = new SequenceFile.Reader( conf, SequenceFile.Reader.file(centersPath));
        SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
                                                                     SequenceFile.Writer.file(centersPath),
                                                                     SequenceFile.Writer.keyClass(Cluster.class),
                                                                     SequenceFile.Writer.valueClass(MeanData.class));
        HashMap<Cluster, MeanData> oldCentroids = new HashMap<Cluster, MeanData>();
        Cluster oldCluster = new Cluster(  );
        MeanData oldMeanData = new MeanData( 1, new Point( 1 ) );
        int count = 0;
        for (; centerReader.next( oldCluster, oldMeanData );)
        {
            oldCentroids.put(oldCluster, oldMeanData);
            ++count;
        }
        if (count == 0)
        {
            throw new IOException("Centroids file seems empty");
        }

        boolean hasConverged = HasConverged( oldCentroids );
        conf.setBoolean( ConfStringHasConverged, hasConverged );
        if (!hasConverged)
        {
            fs.truncate( centersPath, 0 );
            for (HashMap.Entry<Cluster, MeanData> entry : newCentroids.entrySet())
            {
                centerWriter.append( entry.getKey(), entry.getValue() );
            }
        }

        centerReader.close();
        centerWriter.close();
    }
}
