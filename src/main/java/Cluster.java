import javax.ws.rs.Path;

public class Cluster {
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
}
