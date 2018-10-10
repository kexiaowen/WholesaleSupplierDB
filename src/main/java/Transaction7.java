import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Transaction7 {
    class Customer implements Comparable<Customer> {
        int C_W_ID;
        int C_D_ID;
        String C_FIRST;
        String C_MIDDLE;
        String C_LAST;
        double C_BALANCE;

        Customer(int C_W_ID, int C_D_ID, String C_FIRST, String C_MIDDLE, String C_LAST, double C_BALANCE) {
            this.C_W_ID = C_W_ID;
            this.C_D_ID = C_D_ID;
            this.C_FIRST = C_FIRST;
            this.C_MIDDLE = C_MIDDLE;
            this.C_LAST = C_LAST;
            this.C_BALANCE = C_BALANCE;
        }
        public int compareTo(Customer other) {
            if (this.C_BALANCE > other.C_BALANCE) {
                return -1;
            } else if (this.C_BALANCE < other.C_BALANCE) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    Logger logger = LoggerFactory.getLogger(Transaction7.class);

    private Session session;

    public Transaction7(Session session) {
        this.session = session;
    }

    public void execute() {
        List<Customer> topCustomers = new ArrayList<Customer>();
        for (int i = 1; i <= 10; i++) {
            String q1 = String.format("SELECT C_W_ID, C_D_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE " +
                    "FROM Customer " +
                    "WHERE C_W_ID = %d " +
                    "ALLOW FILTERING;", i);
            // logger.info(q1);
            Iterator<Row> iter = session.execute(q1).iterator();
            List<Customer> topCustomersWarehouse = new ArrayList<Customer>();
            while (iter.hasNext()) {
                Row row = iter.next();
                int C_W_ID = row.getInt("C_W_ID");
                int C_D_ID = row.getInt("C_D_ID");
                String C_FIRST = row.getString("C_FIRST");
                String C_MIDDLE = row.getString("C_MIDDLE");
                String C_LAST  = row.getString("C_LAST");

                double C_BALANCE = row.getDecimal("C_BALANCE").doubleValue();
                Customer c = new Customer(C_W_ID, C_D_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE);
                topCustomersWarehouse.add(c);
            }
            Collections.sort(topCustomersWarehouse);
            for (int j = 0; j < 10; j++) {
                topCustomers.add(topCustomersWarehouse.get(j));
            }
        }
        Collections.sort(topCustomers);

        for (int i = 0; i < 10; i++) {
            Customer c = topCustomers.get(i);
            String q2 = String.format(
                    "SELECT W_NAME FROM Warehouse WHERE W_ID = %d;", c.C_W_ID
            );
            String W_NAME = session.execute(q2).one().getString("W_NAME");
            String q3 = String.format(
                    "SELECT D_NAME FROM District WHERE D_W_ID = %d AND D_ID = %d;", c.C_W_ID, c.C_D_ID
            );
            String D_NAME = session.execute(q3).one().getString("D_NAME");
            System.out.printf("Name of Customer: %s %s %s\n", c.C_FIRST, c.C_MIDDLE, c.C_LAST);
            System.out.printf("Balance of customer's outstanding payment: %f\n", c.C_BALANCE);
            System.out.printf("Warehouse name of customer: %s\n", W_NAME);
            System.out.printf("District name of customer: %s\n", D_NAME);
        }
    }
}
