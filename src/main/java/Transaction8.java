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

    public String toString() {
        return String.format("%d, %d, %d", cwid, cdid, cid);
    }
}

public class Transaction8 {

    private int W_ID, D_ID, C_ID;
    private Session session;
    private ArrayList<Customer> relatedCustomer;
    private ArrayList<ArrayList<Integer>> targetOrderLines;

    public Transaction8(Session session, int W_ID, int D_ID, int C_ID) {
        this.session = session;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.C_ID = C_ID;
        relatedCustomer = new ArrayList<Customer>();
        targetOrderLines = new ArrayList<ArrayList<Integer>>();
    }

    private boolean hasTwoSameOrderLine(ArrayList<Integer> OL1, ArrayList<Integer> OL2) {
        int counter = 0;
        for (int i = 0; i < OL1.size(); i++) {
            int iid1 = OL1.get(i);
            for (int j = 0; j < OL2.size(); j++) {
                int iid2 = OL2.get(j);
                if (iid1 == iid2) {
                    counter++;
                    if (counter >= 2)
                        return true;
                    else
                        break;
                }
            }
        }
        return false;
    }

    private boolean hasSatisfiedOrder(int wid, int did, ArrayList<Integer> orders) {
        for (int i = 0; i < orders.size(); i++) {
            String orderLineQuery = String.format(
                    "SELECT OL_I_ID FROM OrderLine WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d;",
                    wid, did, orders.get(i)
            );
            Iterator<Row> orderLineIter = session.execute(orderLineQuery).iterator();
            ArrayList<Integer> orderLines = new ArrayList<Integer>();
            while (orderLineIter.hasNext()) {
                orderLines.add(orderLineIter.next().getInt("OL_I_ID"));
            }
            for (int j = 0; j < targetOrderLines.size(); j++) {
                if (hasTwoSameOrderLine(orderLines, targetOrderLines.get(j))) {
                    return true;
                }
            }
        }
        return false;
    }

    private void findRelatedCustomers(String query) {
        Iterator<Row> customerIterator = session.execute(query).iterator();

        while (customerIterator.hasNext()) {
            Row nextCustomer = customerIterator.next();
            int wid = nextCustomer.getInt("C_W_ID");
            int did = nextCustomer.getInt("C_D_ID");
            int cid = nextCustomer.getInt("C_ID");
            String nextCustomerOrderQuery = String.format(
                    "SELECT O_ID FROM Order_With_CID WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d;",
                    wid, did, cid
            );
            Iterator<Row> orderIter = session.execute(nextCustomerOrderQuery).iterator();
            ArrayList<Integer> orders = new ArrayList<Integer>();
            while (orderIter.hasNext()) {
                orders.add(orderIter.next().getInt("O_ID"));
            }
            if (hasSatisfiedOrder(wid, did, orders)) {
                relatedCustomer.add(new Customer(wid, did, cid));
            }
        }
    }

    private void findTargetOrderLine() {
        String q1 = String.format(
                "SELECT O_ID FROM Order_With_CID WHERE O_W_ID = %d AND O_D_ID = %d AND O_C_ID = %d;",
                W_ID, D_ID, C_ID
        );
        Iterator<Row> targetOrderIterator = session.execute(q1).iterator();
        int numOrder = 0;
        while (targetOrderIterator.hasNext()) {
            int nextOrderId = targetOrderIterator.next().getInt("O_ID");
            targetOrderLines.add(new ArrayList<Integer>());
            String orderLineQuery = String.format(
                    "SELECT OL_I_ID FROM OrderLine WHERE OL_W_ID = %d AND OL_D_ID = %d AND OL_O_ID = %d;",
                    W_ID, D_ID, nextOrderId
            );
            Iterator<Row> orderLineIter = session.execute(orderLineQuery).iterator();
            while (orderLineIter.hasNext()) {
                int orderLineiid = orderLineIter.next().getInt("OL_I_ID");
                targetOrderLines.get(numOrder).add(orderLineiid);
            }
            numOrder++;
        }
    }

    private void printResult() {
        System.out.printf("Target Customer: %d, %d, %d\n", W_ID, D_ID, C_ID);
        for (int i = 0; i < relatedCustomer.size(); i++) {
            System.out.println(relatedCustomer.get(i));
        }
    }

    public void execute() {
        findTargetOrderLine();
        String q2 = "SELECT MAX(W_ID) FROM Warehouse;";
        int maxWID = session.execute(q2).one().getInt("SYSTEM.MAX(W_ID)");
        for (int i = 1; i <= maxWID; i++) {
            if (i == W_ID) continue;
            String q = String.format(
                    "SELECT C_W_ID, C_D_ID, C_ID FROM Customer_By_WID WHERE C_W_ID = %d;", i
            );
            findRelatedCustomers(q);
        }

        printResult();
    }
}
