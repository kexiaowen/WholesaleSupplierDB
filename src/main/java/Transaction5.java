import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.Iterator;
import java.util.TreeSet;

public class Transaction5 {

    private Session session;
    private int W_ID, D_ID, threshold, L;
    private TreeSet<Integer> items;

    public Transaction5(Session session, int W_ID, int D_ID, int threshold, int L) {
        this.session = session;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.threshold = threshold;
        this.L = L;
        items = new TreeSet<>();
    }

    public void execute() {
        String q1 = String.format(
                "SELECT O_ID FROM Order_T5 WHERE O_W_ID = %d AND O_D_ID = %d " +
                        "ORDER BY O_ID DESC LIMIT %d;",
                W_ID, D_ID, L
        );

        Iterator<Row> iterator1 = session.execute(q1).iterator();
        while (iterator1.hasNext()) {
            Row order = iterator1.next();
            int OID = order.getInt("O_ID");
            String q2 = String.format(
                    "SELECT OL_I_ID FROM OrderLine WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d;",
                    W_ID, D_ID, OID
            );
            Iterator<Row> iterator2 = session.execute(q2).iterator();
            while (iterator2.hasNext()) {
                Row orderLine = iterator2.next();
                items.add(orderLine.getInt("OL_I_ID"));
            }
        }

        int result = 0;
        
    }
}
