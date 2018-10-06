import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.util.Scanner;

public class ClientDriver {

    public Cluster cluster;
    public Session session;

    public static void main(String[] args) {
        ClientDriver driver = new ClientDriver();
        String ip = "127.0.0.1";
        driver.connect(ip);
        driver.readInput();
    }

    private void readInput() {
        Scanner sc = new Scanner(System.in);
        while (sc.hasNext()) {
            String[] firstRow = sc.next().split(",");
            char type = firstRow[0].charAt(0);
            switch (type) {
                // N, P, D, O, S, I, T, or R,
                case 'N':
                    int CID1 = Integer.valueOf(firstRow[1]);
                    int WID1 = Integer.valueOf(firstRow[2]);
                    int DID1 = Integer.valueOf(firstRow[3]);
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
                    new Transaction1(session, WID1, DID1, CID1, numItem,
                            item_number, supplier_warehouse, quantity).execute();
                    break;
                case 'P':
                    int WID2 = Integer.valueOf(firstRow[1]);
                    int DID2 = Integer.valueOf(firstRow[2]);
                    int CID2 = Integer.valueOf(firstRow[3]);
                    double payment = Double.valueOf(firstRow[4]);
                    new Transaction2(session, WID2, DID2, CID2, payment).execute();
                    break;
                case 'D':
                    break;
                case 'O':
                    int WID4 = Integer.valueOf(firstRow[1]);
                    int DID4 = Integer.valueOf(firstRow[2]);
                    int CID4 = Integer.valueOf(firstRow[3]);
                    new Transaction4(session, WID4, DID4, CID4).execute();
                    break;
                case 'S':
                    int WID5 = Integer.valueOf(firstRow[1]);
                    int DID5 = Integer.valueOf(firstRow[2]);
                    double threshold = Double.valueOf(firstRow[3]);
                    int L = Integer.valueOf(firstRow[4]);
                    new Transaction5(session, WID5, DID5, threshold, L).execute();
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
    
}
