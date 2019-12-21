import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.Iterator;

public class KmeanReducer extends Reducer<Cluster, MeanData, Cluster, MeanData> {


    protected void reduce(Cluster key, Iterable<MeanData> meanDatas, Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();

        Iterator<MeanData> iterator = meanDatas.iterator();
        MeanData res = iterator.next();
        iterator.remove();
        while(iterator.hasNext()) {
            res = MeanData.Combine(res, iterator.next());
            iterator.remove();
        }

        context.write(key, res);
    }

    protected void cleanup(Context context) {
    }
}
