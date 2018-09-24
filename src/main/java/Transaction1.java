import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import jnr.ffi.annotations.In;

public class Transaction1 {
    private Session session;
    private String W_ID;
    private String D_ID;
    private String C_ID;
    private int num_items;
    private String[] item_number;
    private String[] supplier_warehouse;
    private String[] quantity;

    public Transaction1(Session session, String W_ID, String D_ID, String C_ID, int num_items,
                        String[] item_number, String[] supplier_warehouse, String[] quantity) {
        this.session = session;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.C_ID = C_ID;
        this.num_items = num_items;
        this.item_number = item_number;
        this.supplier_warehouse = supplier_warehouse;
        this.quantity = quantity;
    }

    public Transaction1(String W_ID, String D_ID) {
        this.W_ID = W_ID;
        this.D_ID = D_ID;
    }

    public void execute() {
        String q1 = String.format(
                "SELECT D_NEXT_O_ID FROM District_T1 WHERE D_W_ID = %s AND D_ID = %s;",
                W_ID, D_ID);

        Row row1 = session.execute(q1).one();
        int nextOID = Integer.valueOf(row1.getString("D_NEXT_O_ID"));

        // sout
        System.out.println("nextOID = " + nextOID);

        String q2 = String.format(
                "UPDATE District SET D_NEXT_ID = %d WHERE D_W_ID = %s AND D_ID = %s;",
                nextOID + 1, W_ID, D_ID
        );
        session.execute(q2);

        int all_local = 1;
        for (int i = 0; i < num_items; i++) {
            if (!supplier_warehouse[i].equals(W_ID)) {
                all_local = 0;
                break;
            }
        }

        String q3 = String.format(
                "INSERT INTO Orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) "
                        + "VALUES (%d, %s, %s, %s, toTimeStamp(now()), -1, %d, %d);",
                nextOID, D_ID, W_ID, C_ID, num_items, all_local
        );

        int total_amout = 0;

        

        for (int i = 0; i < num_items; i++) {
            String q = String.format("SELECT S_QUANTITY, ");
        }

        System.out.println(q1);

    }

    public static void main(String[] args) {
        new Transaction1("123", "456").execute();
    }
}
