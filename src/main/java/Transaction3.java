import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.Iterator;

public class Transaction3 {
    private Session session;
    private int W_ID;
    private int CARRIER_ID;
    private int[] O_IDs;
    private int[] C_IDs;

    public Transaction3(Session session, int W_ID, int CARRIER_ID) {
        this.session = session;
        this.W_ID = W_ID;
        this.CARRIER_ID = CARRIER_ID;

        // Since there are 10 districts, there is one oldest undelivered order inside every district.
        this.O_IDs = new int[10];
        this.C_IDs = new int[10];
    }

    public void execute() {
        retrieveAndUpdateOldestUndeliveredOrder();
        updateOrderline();
//        updateCustomer();

        // For this transaction, it does not need to print results
    }

    private void retrieveAndUpdateOldestUndeliveredOrder() {
        for(int i = 1; i <= 10; i++) {
            String q1 = String.format("SELECT t.O_ID, t.O_C_ID FROM Order_T3 t WHERE t.O_W_ID=%d AND t.O_D_ID=%d AND t.O_CARRIER_ID=%d ASC;",
                    W_ID, i, -1
                    );
            Row row1 = session.execute(q1).one();
            int O_ID = row1.getInt("O_ID");
            O_IDs[i] = O_ID;
            C_IDs[i] = row1.getInt("O_C_ID");

            String q2 = String.format("UPDATE Order SET O_CARRIER_ID = %d WHERE O_ID = %d AND O_W_ID = %d AND O_D_ID = %d;",
                    CARRIER_ID, O_ID, W_ID, i
                    );
            session.execute(q2);
        }
    }

    private void updateOrderline() {
        for(int i = 1; i <= 10; i++) {
            String q3 = String.format("UPDATE OrderLine SET OL_DELIVERY_D = toTimestamp(now()) WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d;",
                    W_ID, i, O_IDs[i]
            );

            session.execute(q3);

            String q4 = String.format("SELECT ol.OL_AMOUNT FROM OrderLine_T3 ol WHERE ol.OL_W_ID = %d AND ol.OL_D_ID = %d AND ol.OL_O_ID = %d;",
                    W_ID, i, O_IDs[i]
                    );

            Iterator<Row> it = session.execute(q4).iterator();

            int totalAmount = 0;
            while(it.hasNext()) {
                Row row = it.next();
                totalAmount += row.getInt("OL_AMOUNT");
            }

            updateCustomer(i, totalAmount);
        }
    }

    private void updateCustomer(int districtIndex, int totalAmount) {
        String q5 = String.format("SELECT c.C_BALANCE c.C_DELIVERY_CNT FROM Customer_T3 c WHERE c._W_ID = %d AND c.C_D_ID = %d AND c.C_ID = %d;",
                W_ID, districtIndex, C_IDs[districtIndex]
                );

        Row row = session.execute(q5).one();
        int new_balance = row.getInt("C_BALANCE") + totalAmount;
        int new_delivery_cnt = row.getInt("C_DELIVERY_CNT") + 1;

        String q6 = String.format("UPDATE Customer SET C_BALANCE = %d, C_DELIVERY_CNT = %d WHERE c._W_ID = %d AND c.C_D_ID = %d AND c.C_ID = %d;",
                new_balance, new_delivery_cnt, W_ID, districtIndex, C_IDs[districtIndex]
                );

        session.execute(q6);
    }
}
