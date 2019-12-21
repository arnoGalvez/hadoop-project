import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KmeansReducer extends Reducer<Cluster, MeanData, Cluster, MeanData> {
    /*private IntWritable result = new IntWritable();

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
    }*/

    public void reduce(Cluster cluster,
                       Iterable<MeanData> means,
                       Context context) throws IOException, InterruptedException
    {
        MeanData result = means.iterator().next();
        while(means.iterator().hasNext())
        {
            result = MeanData.Combine( result, means.iterator().next() );
        }
        context.write( cluster, result );
    }
}
