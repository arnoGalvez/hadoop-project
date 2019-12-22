import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
        if (centroidsRm.exists( centers ))
        {
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
        while(hasConverged == 0)
        {
            Job job = Job.getInstance( conf, "Kmeans compute" );
            job.setJarByClass( Kmeans.class );
            job.setMapperClass( KmeansMapper.class );
            job.setNumReduceTasks( 1 );
            //job.setCombinerClass( IntSumReducer.class );

            job.setReducerClass( KmeansReducer.class );
            job.setOutputKeyClass( Cluster.class );
            job.setOutputValueClass( MeanData.class );

            FileInputFormat.addInputPath( job, input );
            FileOutputFormat.setOutputPath( job, centers );
            job.waitForCompletion( true );

            hasConverged = job.getCounters().findCounter( KmeansReducer.CONVERGENCE_COUNTER.COUNTER ).getValue();
        }

        Job writeCluster = Job.getInstance( conf, "Write clusters" );
        writeCluster.setMapperClass(FinalMapper.class);
        writeCluster.setReducerClass(FinalReducer.class);
        FileInputFormat.addInputPath( writeCluster, centers );
        FileOutputFormat.setOutputPath(writeCluster, output);

        FileSystem fileSystem = FileSystem.get( centers.toUri(), conf );
        fileSystem.delete( centers, true );
        fileSystem.close();

        System.exit( 0 );
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static int k;
        private static int col;// Coordinates starting columns
        private static int coordinatesCount;
        private final static IntWritable one = new IntWritable( 1 );
        private Text word = new Text();

        public void setup (Context context) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            k   = conf.getInt("k", -1);
            col = conf.getInt("col", -1);
            coordinatesCount = conf.getInt("coordinatesCount", 0);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            /* Get all columns */
            String[] tokens = value.toString().split(",");

            StringTokenizer itr = new StringTokenizer( value.toString(), "," );
            while ( itr.hasMoreTokens() ) {
                /*word.set( itr.nextToken() );
                context.write( word, one );*/
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
