import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.util.Scanner;

public class ClientDriver {

    public static Cluster cluster;
    public static Session session;

    public static void main() {
        ClientDriver driver = new ClientDriver();
        driver.connect();

        //Transaction1 t1 = new Transaction1(session, );
        //t1.execute();
    }

    private void readInput() {
        Scanner sc = new Scanner(System.in);
        String[] firstRow = sc.next().split(",");
        char type = firstRow[0].charAt(0);
        switch (type) {
            //N, P, D, O, S, I, T, or R,
            case 'N':
                break;
            case 'P':
                break;
            case 'D':
                break;
            case 'O':
                break;
            case 'S':
                break;
            case 'I':
                break;
            case 'T':
                break;
            case 'R':
                break;
        }
    }

    private void connect() {
        cluster = Cluster.builder()
                .withClusterName("Test Cluster")
                .addContactPoint("192.168.48.219")
                .build();

        session = cluster.connect("WholesaleSupplierDB");
    }

    // ResultSet rset = session.execute("select * from myTable where id = 1");

}
