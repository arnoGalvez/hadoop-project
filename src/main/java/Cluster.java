import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import javax.ws.rs.Path;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Cluster implements WritableComparable<Cluster> {

    private int id;

    Cluster() {id = -1;}
    Cluster(int id)
    {
        this.id = id;
    }

    public int GetId()
    {
        return id;
    }

    public static void InitClusterFile(Path path, int k)
    {

    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt( id );
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        id = in.readInt();
    }

    @Override
    public int compareTo(Cluster cluster)
    {
        return id < cluster.id ? -1 : id == cluster.id ? 0 : 1;
    }
}
