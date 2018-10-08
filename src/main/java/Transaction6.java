import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.sun.xml.internal.bind.v2.TODO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;

// Havent tested on server, having some issues connecting to server

public class Transaction6 {

    Logger logger = LoggerFactory.getLogger(Transaction6.class);

    private Session session;
    private int W_ID;
    private int D_ID;
    private int L;

    public Transaction6(Session session, int W_ID, int D_ID, int L) {
        this.session = session;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.L = L;
    }

    public void execute() {
        printResult();
        String q1 = String.format("SELECT D_NEXT_O_ID FROM District WHERE D_W_ID=%d AND D_ID=%d;", W_ID, D_ID);
        Row row1 = session.execute(q1).one();
        int next_o_id = row1.getInt("NEXT_O_ID");

        ArrayList<Integer> popular_items = new ArrayList<Integer>();
        ArrayList<String> popular_item_names = new ArrayList<String>();

        for(int i = next_o_id-1; i >= next_o_id-L; i++) {
            String q2 = String.format("SELECT O_ENTRY_D, O_C_ID FROM Orders WHERE O_W_ID=%d AND O_D_ID=%d AND O_ID=%d;", W_ID, D_ID, i);
            Row row2 = session.execute(q2).one();
            System.out.printf("The Order number is %d and the entry date and number is %s\n", i, row2.getTimestamp("O_ENTRY_D").toString());

            // print name of customer
            String q3 = String.format("SELECT C_FIRST, C_MIDDLE, C_LAST FROM Customer WHERE C_W_ID=%d and C_D_ID=%d AND C_ID=%d;", W_ID, D_ID, row2.getInt("O_C_ID"));
            Row row3 = session.execute(q3).one();
            System.out.printf("The name of customer who placed this order: %s %s %s.\n",
                    row3.getString("C_FIRST"), row3.getString("C_MIDDLE"), row3.getString("C_LAST")
            );

            ArrayList<Row> OLs = new ArrayList<Row>();
            // TODO: modify the attributes later to only select necessary attributes
            String q4 = String.format("SELECT * FROM OrderLine WHERE OL_W_ID=%d AND OL_D_ID=%d AND OL_O_ID=%d;", W_ID, D_ID, i);

            Iterator<Row> iterator = session.execute(q4).iterator();
            while(iterator.hasNext()) {
                OLs.add(iterator.next());
            }

            // record OL id and quantity for the most popular item
            int max = 0;
            // may have more than one popular item, hence use a list to store the intermediate result
            ArrayList<Integer> item_ids = new ArrayList<Integer>();

            // compare all the order lines within one order to find out the most popular items
            for(int j = 0; j < OLs.size(); j++) {
                Row row4 = OLs.get(j);
                int ol_quantity = row4.getDecimal("OL_QUANTITY").intValue();
                if(ol_quantity > max) {
                    item_ids = new ArrayList<Integer>();
                    item_ids.add(row4.getInt("OL_O_ID"));
                    max = ol_quantity;
                } else if(ol_quantity == max) {
                    item_ids.add(row4.getInt("OL_O_ID"));
                }
            }

            // After the selection, item_ids contains the id of popular item for the current order
            for(int j = 0; j < item_ids.size(); j++) {
                popular_items.add(item_ids.get(j));
                String q5 = String.format("SELECT I_NAME FROM Item WHERE I_ID=%d;", item_ids.get(j));
                Row row5 = session.execute(q5).one();
                System.out.printf("Item name: %s\n Quantity ordered: %d\n", row5.getString("I_NAME"), max);
                popular_item_names.add(row5.getString("I_NAME"));
            }
        }

        // calculate the additional information that will be printed later
        for(int i = 0; i < popular_items.size(); i++) {
            int count = 0;
            for(int j = next_o_id-1; j >= next_o_id-L; j++) {
                String q6 = String.format("SELECT OL_I_ID FROM OrderLine_T6 WHERE OL_W_ID=%d AND OL_D_ID=%d AND OL_O_ID=%d AND OL_I_ID=%d;",
                        W_ID, D_ID, j, popular_items.get(i)
                );

                Row row6 = session.execute(q6).one();
                if(row6 != null) {
                    count++;
                }
            }
            System.out.printf("Item name: %s, percent of orders that contain the popular item: %f\n", popular_item_names.get(i), count*1.0/L);
        }
    }

    private void printResult() {
        System.out.printf("District identifier: W_ID: %d, D_ID: %d\n", W_ID, D_ID);
        System.out.printf("Number of orders to execute: %d\n", L);
    }
}
