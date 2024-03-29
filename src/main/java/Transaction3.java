import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

// Havent tested on server

public class Transaction3 {

    Logger logger = LoggerFactory.getLogger(Transaction3.class);

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
        this.O_IDs = new int[11];
        this.C_IDs = new int[11];
    }

    public void execute() {
        retrieveAndUpdateOldestUndeliveredOrder();
        updateOrderline();
    }

    private void retrieveAndUpdateOldestUndeliveredOrder() {
        for(int i = 1; i <= 10; i++) {
            String q1 = String.format("SELECT O_ID, O_C_ID FROM Order_T3 " +
                            "WHERE O_W_ID=%d AND O_D_ID=%d AND O_CARRIER_ID=%d ORDER BY O_ID ASC LIMIT 1;",
                    W_ID, i, -1
            );
            Row row1 = session.execute(q1).one();
            int O_ID = row1.getInt("O_ID");
            O_IDs[i] = O_ID;
            C_IDs[i] = row1.getInt("O_C_ID");

            String q2 = String.format("UPDATE Orders SET O_CARRIER_ID = %d " +
                            "WHERE O_ID = %d AND O_W_ID = %d AND O_D_ID = %d;",
                    CARRIER_ID, O_ID, W_ID, i
            );
            session.execute(q2);
        }
    }

    private void updateOrderline() {
        for(int i = 1; i <= 10; i++) {
            String q3 = String.format("SELECT O_OL_CNT FROM Orders WHERE O_W_ID=%d AND O_D_ID=%d AND O_ID=%d;",
                    W_ID, i, O_IDs[i]
            );
            Row count_row = session.execute(q3).one();
            int num_items = count_row.getDecimal("O_OL_CNT").intValue();

            for(int j = 1; j <= num_items; j++) {
                String q4 = String.format("UPDATE OrderLine SET OL_DELIVERY_D = toTimestamp(now()) " +
                                "WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d AND OL_NUMBER=%d;",
                        W_ID, i, O_IDs[i], j
                );

                session.execute(q4);
            }
            String q5 = String.format("SELECT OL_AMOUNT FROM OrderLine " +
                            "WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d;",
                    W_ID, i, O_IDs[i]
            );
            Iterator<Row> it = session.execute(q5).iterator();

            double totalAmount = 0;
            while(it.hasNext()) {
                Row row = it.next();
//                if(!row.isNull("OL_AMOUNT")) {
                totalAmount += row.getDecimal("OL_AMOUNT").doubleValue();
//                }
            }

            updateCustomer(i, totalAmount);
        }
    }

    private void updateCustomer(int districtIndex, double totalAmount) {
        String q6 = String.format("SELECT C_BALANCE, C_DELIVERY_CNT FROM Customer " +
                        "WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
                W_ID, districtIndex, C_IDs[districtIndex]
        );

        Row row = session.execute(q6).one();
        double new_balance = row.getDecimal("C_BALANCE").doubleValue() + totalAmount;
        int new_delivery_cnt = row.getInt("C_DELIVERY_CNT") + 1;

        String q7 = String.format("UPDATE Customer SET C_BALANCE = %f, C_DELIVERY_CNT = %d " +
                        "WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
                new_balance, new_delivery_cnt, W_ID, districtIndex, C_IDs[districtIndex]
        );

        session.execute(q7);
    }
}
