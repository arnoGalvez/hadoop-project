import org.apache.hadoop.io.Writable;

import javax.ws.rs.Path;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Cluster implements Writable {
    private int id;
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
}
