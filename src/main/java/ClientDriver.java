import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class ClientDriver {

    public static Cluster cluster;
    public static Session session;

    public static void main() {
        connect();

        //Transaction1 t1 = new Transaction1(session, );
        //t1.execute();
    }

    public static void connect() {
        cluster = Cluster.builder()
                .withClusterName("Test Cluster")
                .addContactPoint("192.168.48.219")
                .build();

        session = cluster.connect("WholesaleSupplierDB");
    }

    // ResultSet rset = session.execute("select * from myTable where id = 1");

}
