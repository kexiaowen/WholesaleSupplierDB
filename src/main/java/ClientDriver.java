import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import jnr.ffi.annotations.In;

import java.util.Scanner;

public class ClientDriver {

    public Cluster cluster;
    public Session session;

    public static void main(String[] args) {
        ClientDriver driver = new ClientDriver();
        String ip = "127.0.0.1";
        driver.connect(ip);

        //Transaction1 t1 = new Transaction1(session, );
        //t1.execute();
    }

    private void readInput() {
        Scanner sc = new Scanner(System.in);
        while (sc.hasNext()) {
            String[] firstRow = sc.next().split(",");
            char type = firstRow[0].charAt(0);
            switch (type) {
                // N, P, D, O, S, I, T, or R,
                case 'N':
                    int CID = Integer.valueOf(firstRow[1]);
                    int WID = Integer.valueOf(firstRow[2]);
                    int DID = Integer.valueOf(firstRow[3]);
                    int numItem = Integer.valueOf(firstRow[4]);
                    int[] item_number = new int[numItem];
                    int[] supplier_warehouse = new int[numItem];
                    int[] quantity = new int[numItem];
                    for (int i = 0; i < numItem; i++) {
                        String[] row = sc.next().split(",");
                        item_number[i] = Integer.valueOf(row[0]);
                        supplier_warehouse[i] = Integer.valueOf(row[1]);
                        quantity[i] = Integer.valueOf(row[2]);
                    }
                    new Transaction1(session, WID, DID, CID, numItem,
                            item_number, supplier_warehouse, quantity).execute();
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
    }

    private void connect(String ip) {
        cluster = Cluster.builder()
                .withClusterName("Test Cluster")
                .addContactPoint(ip)
                .build();

        session = cluster.connect("WholesaleSupplierDB");
    }

    // ResultSet rset = session.execute("select * from myTable where id = 1");

}
