import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

import java.util.HashMap;
import java.util.Iterator;

public class KmeansReducer extends Reducer<Cluster, MeanData, Cluster, MeanData> {

    static private HashMap<Integer, MeanData> newCentroids = new HashMap<Integer, MeanData>();

    static public final String ConfStringHasConverged = "HasConverged";

    public enum CONVERGENCE_COUNTER {COUNTER}

    static int iterationCount = 0;

    boolean HasConverged(HashMap<Integer, MeanData> oldCentroids, int expectedIterations) throws IOException
    {
        ++iterationCount;

        final double eps = 99999999999.999;
        Iterator<Integer> clusterIterator = newCentroids.keySet().iterator();
        int k = 0;
        while (clusterIterator.hasNext())
        {
            Integer cluster = clusterIterator.next();
            Point point1 = newCentroids.get( cluster ).ComputeMean();
            Point point2 = oldCentroids.get( cluster ).ComputeMean();
            Point vec = Point.sub( point1, point2 );
            double sqrDist = vec.norm();
            if (sqrDist > eps)
            {
                return false;
            }
            ++k;

        }
        if (k != expectedIterations)
        {
            throw new IOException( "Wrong number of clusters. Was " + k + " expected " + expectedIterations + ".\n centroids: " + newCentroids.toString() );
        }

        if (iterationCount == 1)
        {
            throw  new IOException( "Weird convergence in only 1 iteration. Oldcentroids :\n" + oldCentroids.toString() + "\nNewCentroids: " + newCentroids.toString() );
        }

        return true;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup( context );
        Configuration conf = context.getConfiguration();
        Path centersPath = new Path(conf.get("centroids"));

        SequenceFile.Reader centerReader = new SequenceFile.Reader( conf, SequenceFile.Reader.file(centersPath));

        Cluster oldCluster = new Cluster(  );
        MeanData oldMeanData = new MeanData( 1, new Point( 1 ) );
        int count = 0;
        while (centerReader.next( oldCluster, oldMeanData ))
        {
            newCentroids.put(new Integer( oldCluster.GetId() ), oldMeanData);
            ++count;
        }
        if (count == 0 || count != conf.getInt( "k", 0 ))
        {
            centerReader.close();
            throw new IOException("Centroids file seems empty");
        }

        centerReader.close();

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
        newCentroids.put( new Integer( cluster.GetId() ), result );
        context.write( cluster, result );
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        Path centersPath = new Path(conf.get("centroids"));



        SequenceFile.Reader centerReader = new SequenceFile.Reader( conf, SequenceFile.Reader.file(centersPath));

        HashMap<Integer, MeanData> oldCentroids = new HashMap<Integer, MeanData>();
        Cluster oldCluster = new Cluster(  );
        MeanData oldMeanData = new MeanData( 1, new Point( 1 ) );
        int count = 0;
        int lastId = -1;
        while (centerReader.next( oldCluster, oldMeanData ))
        {
            if (oldCluster.GetId() == lastId)
            {
                throw new IOException( "Read the same cluster two times. Cluster was " + lastId );
            }
            lastId = oldCluster.GetId();
            oldCentroids.put(new Integer( oldCluster.GetId() ), oldMeanData);
            ++count;
        }
        if (count == 0 || count != conf.getInt( "k", 0 ))
        {
            throw new IOException("Centroids file seems empty");
        }

        centerReader.close();

        boolean hasConverged = HasConverged( oldCentroids, conf.getInt( "k", -1 ) );

        context.getCounter( CONVERGENCE_COUNTER.COUNTER ).setValue( hasConverged ? 1 : 0 );

        if (!hasConverged)
        {
            FileSystem fs = FileSystem.get( centersPath.toUri(), conf );
            fs.truncate( centersPath, 0 );
            fs.close();
            SequenceFile.Writer centerWriter = SequenceFile.createWriter(conf,
                                                                         SequenceFile.Writer.file(centersPath),
                                                                         SequenceFile.Writer.keyClass(Cluster.class),
                                                                         SequenceFile.Writer.valueClass(MeanData.class));

            for (HashMap.Entry<Integer, MeanData> entry : newCentroids.entrySet())
            {
                centerWriter.append( new Cluster( entry.getKey()), entry.getValue() );
            }
            centerWriter.close();
        }



    }
}
