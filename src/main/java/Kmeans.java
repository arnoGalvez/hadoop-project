import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Kmeans {

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();


        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Path centers = new Path(input.getParent().toString() + "/centroids");
        Path centersout = new Path(input.getParent().toString() + "/centroidsOut");

        conf.setBoolean(KmeansReducer.ConfStringHasConverged, false);


        /* Set via configuration 'k' and the column onto do the clustering */
        int k   = Integer.parseInt(args[2]);
        int col = Integer.parseInt(args[3]);
        int coordsCount = Integer.parseInt( args[4]);


        conf.setInt("k", k);
        conf.setInt("col", col);
        conf.setInt( "coordinatesCount", coordsCount );
        conf.set("centroids", centers.toString());

        FileSystem outputRm = FileSystem.get(output.toUri(), conf);
        FileSystem centroidsRm = FileSystem.get(centers.toUri(), conf);

        if(outputRm.exists(output)) {
            outputRm.delete(output, true);
        }
        if (centroidsRm.exists( centers )) {
            centroidsRm.delete( centers, true );
        }

        centroidsRm.close();;
        outputRm.close();
        // Default values for centroids

        SequenceFile.Writer centerWriter = SequenceFile.createWriter( conf,
                SequenceFile.Writer.file(centers),
                SequenceFile.Writer.keyClass(Cluster.class),
                SequenceFile.Writer.valueClass(MeanData.class));
        for (int i = 0; i < k; ++i)
        {
            Cluster cluster = new Cluster( i );
            MeanData meanData = new MeanData( 1, Point.RandomPoint( 1, -100.0, 100.0 ) );
            centerWriter.append( cluster, meanData );
        }

        centerWriter.close();


        // Main loop
        long hasConverged = 0;
        // while(hasConverged == 0)
        // {
        FileSystem centreOutRm = FileSystem.get(centersout.toUri(), conf);
        if (centreOutRm.exists(centersout)) {
            centreOutRm.delete(centersout, true);
        }
        centreOutRm.close();
        Job job = Job.getInstance( conf, "Kmeans compute" );
        job.setJarByClass( Kmeans.class );
        job.setMapperClass( KmeansMapper.class );
        job.setNumReduceTasks( 1 );
        //job.setCombinerClass( IntSumReducer.class );

        job.setReducerClass( KmeansReducer.class );
        job.setOutputKeyClass( Cluster.class );
        job.setOutputValueClass( MeanData.class );

        FileInputFormat.addInputPath( job, input );
        FileOutputFormat.setOutputPath( job, centersout );
        job.waitForCompletion( true );

        hasConverged = job.getCounters().findCounter( KmeansReducer.CONVERGENCE_COUNTER.COUNTER ).getValue();
        // }

        Job writeCluster = Job.getInstance( conf, "Write clusters" );
        writeCluster.setJarByClass( Kmeans.class );
        writeCluster.setMapperClass(FinalMapper.class);
        writeCluster.setReducerClass(FinalReducer.class);
        FileInputFormat.addInputPath( writeCluster, input );
        FileOutputFormat.setOutputPath(writeCluster, output);

        writeCluster.setOutputKeyClass( Cluster.class );
        writeCluster.setOutputValueClass( Text.class );


        writeCluster.waitForCompletion( true );

        FileSystem fileSystem = FileSystem.get( centersout.toUri(), conf );
        fileSystem.delete( centersout, true );
        fileSystem.close();

        System.exit( 0 );
    }

    public static class ClusterMapper extends Mapper<Object, Text, Cluster, Text> {
        private static int k;
        private static int col;// Coordinates starting columns
        private static int coordinatesCount;
        private static List<Point> oldcentroids = new ArrayList<Point>();
        private static final Log LOG = LogFactory.getLog(FinalMapper.class);

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

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key,
                           Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for ( IntWritable val : values ) {
                sum += val.get();
            }
            result.set( sum );
            context.write( key, result );
        }
    }
}
