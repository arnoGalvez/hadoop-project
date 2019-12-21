import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
        Path centers = new Path(input.getParent().toString() + "centers/centroids");

        conf.set("centersFilePath", centers.toString());
        conf.setBoolean(KmeansReducer.ConfStringHasConverged, false);

        /* Set via configuration 'k' and the column onto do the clustering */
        int k   = Integer.parseInt(args[2]);
        int col = Integer.parseInt(args[3]);

        conf.setInt("k", k);
        conf.setInt("col", col);
        conf.set("centroids", "centroids0");

        Job job = Job.getInstance( conf, "word pointsCount" );
        job.setJarByClass( Kmeans.class );
        job.setMapperClass( KmeansMapper.class );
        //job.setCombinerClass( IntSumReducer.class );

        job.setReducerClass( KmeansReducer.class );
        job.setOutputKeyClass( Cluster.class );
        job.setOutputValueClass( MeanData.class );

        FileInputFormat.addInputPath( job, input );
        FileOutputFormat.setOutputPath( job, output );

        // Main while loop here

        System.exit( job.waitForCompletion( true ) ? 0 : 1 );
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