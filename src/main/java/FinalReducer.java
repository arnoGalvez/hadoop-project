import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class FinalReducer extends Reducer<Cluster, Text, Cluster, Text> {
    public void reduce(Cluster cluster,
                       Iterable<Text> rows,
                       Context context) throws IOException, InterruptedException
    {
        String ending = "," + cluster.GetId();
        for(Text row : rows) {
            String content = row.toString();
            content += ending;
            Text output = new Text(content);
            context.write(cluster, output);
        }
    }
}
