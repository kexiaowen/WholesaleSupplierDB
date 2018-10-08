import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.Iterator;

class Customer {
    public int cwid, cdid, cid;
    public Customer(int cwid, int cdid, int cid) {
        this.cwid = cwid;
        this.cdid = cdid;
        this.cid = cid;
    }
}

public class Transaction8 {

    private int W_ID, D_ID, C_ID;
    private Session session;
    private ArrayList<Customer> relatedCustomer;
    private ArrayList<ResultSet> targetOrderLines;

    public Transaction8(Session session, int W_ID, int D_ID, int C_ID) {
        this.session = session;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.C_ID = C_ID;
        relatedCustomer = new ArrayList<Customer>();
        targetOrderLines = new ArrayList<ResultSet>();
    }

    private boolean hasSatisfiedOrder(ResultSet order) {
        Iterator<Row> orderIterator = order.iterator();
        while (orderIterator.hasNext()) {
            int oid = orderIterator.next().getInt("O_ID");
            String orderLineQuery = String.format(
                    "SELECT OL_I_ID FROM OrderLine WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d;",
                    W_ID, D_ID, oid
            );
            ResultSet orderLines = session.execute(orderLineQuery);
            for (int i = 0; i < targetOrderLines.size(); i++) {
                if (hasTwoSameOrderLine(orderLines, targetOrderLines.get(i)));
                return true;
            }
        }
        return false;
    }

    private boolean hasTwoSameOrderLine(ResultSet OL1, ResultSet OL2) {
        int counter = 0;
        Iterator<Row> ol1Iterator = OL1.iterator();
        Iterator<Row> ol2Iterator = OL2.iterator();
        while (ol1Iterator.hasNext()) {
            int iid1 = ol1Iterator.next().getInt("OL_I_ID");
            while (ol2Iterator.hasNext()) {
                int iid2 = ol2Iterator.next().getInt("OL_I_ID");
                if (iid1 == iid2) {
                    counter++;
                    if (counter >= 2) return true;
                    else break;
                }
            }
        }
        return false;
    }

    private void findRelatedCustomers() {
        String q2 = String.format(
                "SELECT C_W_ID, C_D_ID, C_ID FROM Customer_By_WID WHERE C_W_ID != %d;", W_ID
        );
        Iterator<Row> customerIterator = session.execute(q2).iterator();

        while (customerIterator.hasNext()) {
            Row nextCustomer = customerIterator.next();
            int wid = nextCustomer.getInt("C_W_ID");
            int did = nextCustomer.getInt("C_D_ID");
            int cid = nextCustomer.getInt("C_ID");
            String nextCustomerOrderQuery = String.format(
                    "SELECT O_ID FROM Order_With_CID WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d;",
                    wid, did, cid
            );
            ResultSet nextCustomerOrders = session.execute(nextCustomerOrderQuery);
            if (hasSatisfiedOrder(nextCustomerOrders)) {
                relatedCustomer.add(new Customer(wid, did, cid));
            }
        }
    }

    private void findTargetOrderline() {
        String q1 = String.format(
                "SELECT O_ID FROM Order_With_CID WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d;",
                W_ID, D_ID, C_ID
        );
        Iterator<Row> targetOrderIterator = session.execute(q1).iterator();
        while (targetOrderIterator.hasNext()) {
            int nextOrderId = targetOrderIterator.next().getInt("O_ID");
            String orderLineQuery = String.format(
                    "SELECT OL_I_ID FROM OrderLine WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d;",
                    W_ID, D_ID, nextOrderId
            );
            ResultSet orderLine = session.execute(orderLineQuery);
            targetOrderLines.add(orderLine);
        }
    }

    private void printResult() {
        System.out.printf("Target Customer: %d, %d, %d\n", W_ID, D_ID, C_ID);
        for (int i = 0; i < relatedCustomer.size(); i++) {
            Customer customer = relatedCustomer.get(i);
            System.out.printf("%d, %d, %d\n", customer.cwid, customer.cdid, customer.cid);
        }
    }

    public void execute() {
        findTargetOrderline();
        findRelatedCustomers();
        printResult();
    }
}
