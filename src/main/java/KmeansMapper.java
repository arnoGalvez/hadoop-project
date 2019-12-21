import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


class KmeansMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Cluster, MeanData> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {

    }
}
