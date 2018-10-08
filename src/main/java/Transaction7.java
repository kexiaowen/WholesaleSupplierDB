import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Transaction7 {
    Logger logger = LoggerFactory.getLogger(Transaction7.class);

    private Session session;

    public Transaction7(Session session) {
        this.session = session;
    }

    public void execute() {
        String q1 = String.format(
                "SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, C_W_ID, C_D_ID " +
                        "FROM Customer_T7 " +
                        "ORDER BY C_BALANCE DESC " +
                        "LIMIT 10;"
        );
        Iterator<Row> iter = session.execute(q1).iterator();

        while(iter.hasNext()) {
            Row row = iter.next();
            String C_FIRST = row.getString("C_FIRST");
            String C_MIDDLE = row.getString("C_MIDDLE");
            String C_LAST  = row.getString("C_LAST");
            double C_BALANCE = row.getDecimal("C_BALANCE").doubleValue();
            int C_W_ID = row.getInt("C_W_ID");
            int C_D_ID = row.getInt("C_D_ID");

            String q2 = String.format(
                    "SELECT W_NAME FROM Warehouse WHERE W_ID = %d;", C_W_ID
            );
            String W_NAME = session.execute(q2).one().getString("W_NAME");
            String q3 = String.format(
                    "SELECT D_NAME FROM District WHERE D_ID = %d;", C_D_ID
            );
            String D_NAME = session.execute(q3).one().getString("D_NAME");
            System.out.printf("Name of Customer: %s %s %s\n", C_FIRST, C_MIDDLE, C_LAST);
            System.out.printf("Balance of customer's outstanding payment: %f\n", C_BALANCE);
            System.out.printf("Warehouse name of customer: %s\n", W_NAME);
            System.out.printf("District name of customer: %s\n", D_NAME);
        }


    }
}
