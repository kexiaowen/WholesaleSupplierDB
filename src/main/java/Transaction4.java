import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.Iterator;

public class Transaction4 {

    private int C_W_ID, C_D_ID, C_ID;
    private Session session;

    public Transaction4(Session session, int C_W_ID, int C_D_ID, int C_ID) {
        this.session = session;
        this.C_W_ID = C_W_ID;
        this.C_D_ID = C_D_ID;
        this.C_ID = C_ID;
    }

    public void execution() {
        String q1 = String.format(
                "SELECT C_FIRST, C_LAST, C_MIDDLE, C_BALANCE FROM Customer WHERE " +
                        "C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
                C_W_ID, C_D_ID, C_ID
        );
        Row row1 = session.execute(q1).one();
        System.out.printf("Name: %s %s %s. Balance: %f\n",
                row1.getString("C_LAST"), row1.getString("C_MIDDLE"), row1.getString("C_FIRST"),
                row1.getDouble("C_BALANCE"));

        String q2 = String.format(
                "SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM Order_With_CID" +
                        "WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d " +
                        "ORDER BY O_ID DESC LIMIT 1;",
                C_W_ID, C_D_ID, C_ID
        );
        Row row2 = session.execute(q2).one();
        int OID = row2.getInt("O_ID");
        System.out.printf("O_ID: %d, O_ENTRY_D: %s, O_CARRIER_ID: %d\n", OID,
                row2.getTimestamp("O_ENTRY_D").toString(), row2.getInt("O_CARRIER_ID"));

        String q3 = String.format(
                "SELECT OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY " +
                        "FROM OrderLine Where OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d;",
                C_W_ID, C_D_ID, OID
        );
        Iterator<Row> iterator3 = session.execute(q3).iterator();
        while (iterator3.hasNext()) {
            Row row = iterator3.next();
            System.out.printf("OL_I_ID: %d, OL_SUPPLY_W_ID: %d, OL_QUANTITY: %d," +
                    " OL_AMOUNT: %f, OL_DELIVERY_D: %s\n",
                    row.getInt("OL_I_ID"), row.getInt("OL_SUPPLY_W_ID"),
                    row.getInt("OL_QUANTITY"), row.getDouble("OL_AMOUNT"),
                    row.getTimestamp("OL_DELIVERY_D").toString());
        }

    }
}
