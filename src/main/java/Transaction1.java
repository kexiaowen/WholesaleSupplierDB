import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import jnr.ffi.annotations.In;

public class Transaction1 {
    private Session session;
    private int W_ID;
    private int D_ID;
    private int C_ID;
    private int num_items;
    private int[] item_number;
    private int[] supplier_warehouse;
    private int[] quantity;

    public Transaction1(Session session, int W_ID, int D_ID, int C_ID, int num_items,
                        int[] item_number, int[] supplier_warehouse, int[] quantity) {
        this.session = session;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.C_ID = C_ID;
        this.num_items = num_items;
        this.item_number = item_number;
        this.supplier_warehouse = supplier_warehouse;
        this.quantity = quantity;
    }

    public Transaction1(int W_ID, int D_ID) {
        this.W_ID = W_ID;
        this.D_ID = D_ID;
    }

    private int retrieveAndUpdateOID() {
        String q1 = String.format(
                "SELECT D_NEXT_O_ID FROM District_T1 WHERE D_W_ID = %d AND D_ID = %d;",
                W_ID, D_ID);

        Row row1 = session.execute(q1).one();
        int nextOID = Integer.valueOf(row1.getString("D_NEXT_O_ID"));

        // sout
        //System.out.println("nextOID = " + nextOID);
        String q2 = String.format(
                "UPDATE District SET D_NEXT_ID = %d WHERE D_W_ID = %d AND D_ID = %d;",
                nextOID + 1, W_ID, D_ID
        );
        session.execute(q2);
        return nextOID;
    }

    private void insertNewOrder(int nextOID) {
        int all_local = 1;
        for (int i = 0; i < num_items; i++) {
            if (supplier_warehouse[i] != W_ID) {
                all_local = 0;
                break;
            }
        }

        String q1 = String.format(
                "INSERT INTO Orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) "
                        + "VALUES (%d, %d, %d, %d, %s, %d, %d, %d);",
                W_ID, D_ID, nextOID, C_ID, "toTimeStamp(now())", -1, num_items, all_local
        );
        session.execute(q1);
    }

    private int insertOrderLinesAndComputePrice(int O_ID) {
        int total_amount = 0;

        for (int i = 0; i < num_items; i++) {
            String q1 = String.format(
                    "SELECT * FROM Stock_T1 WHERE S_W_ID = %d AND S_I_ID = %d;",
                    supplier_warehouse[i], item_number[i]);
            Row row1 = session.execute(q1).one();
            int adjustedQuantity = row1.getInt("S_QUANTITY") - quantity[i];
            int ytd = row1.getInt("S_YTD") + quantity[i];
            int cnt = row1.getInt("S_ORDER_CNT") + 1;
            int remoteCnt = row1.getInt("S_REMOTE_CNT");
            String distInfo = row1.getString("S_DIST_" + D_ID);
            if (supplier_warehouse[i] != W_ID) {
                remoteCnt++;
            }
            if(adjustedQuantity < 10) {
                adjustedQuantity += 100;
            }
            String q2 = String.format(
                    "UPDATE Stock SET S_QUANTITY = %d, S_YTD = %d, S_ORDER_CNT = %d, S_REMOTE_CNT = %d"
                            + "WHERE S_W_ID = %d AND S_I_ID = %d;",
                    adjustedQuantity, ytd, cnt, remoteCnt, supplier_warehouse[i], item_number[i]
            );
            session.execute(q2);

            String q3 = String.format("SELECT * FROM Item_T1 Where I_ID = %d", item_number[i]);
            int price = session.execute(q3).one().getInt("I_PRICE");
            int itemAmount = quantity[i] * price;
            total_amount += itemAmount;

            String q4 = String.format(
                    "INSERT INTO OrderLine (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D,"
                            + " OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO)"
                            + "VALUES (%d, %d, %d, %d, %d, %d, %d, %d, %d, %s)",
                    W_ID, D_ID, O_ID, i, item_number[i], -1,
                    itemAmount, supplier_warehouse[i], quantity[i], distInfo
            );
            session.execute(q4);
        }

        return total_amount;
    }

    private void printResult() {

    }

    public void execute() {
        int nextOID = retrieveAndUpdateOID();
        insertNewOrder(nextOID);
        // End of last meeting
        int rawAmount = insertOrderLinesAndComputePrice(nextOID);
        printResult();
    }

    public static void main(String[] args) {
        new Transaction1(123, 456).execute();
    }
}
