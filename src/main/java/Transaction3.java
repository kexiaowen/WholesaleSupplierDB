import com.datastax.driver.core.Session;

public class Transaction3 {
    private Session session;
    private int W_ID;
    private int CARRIER_ID;

    public Transaction3(Session session, int W_ID, int CARRIER_ID) {
        this.session = session;
        this.W_ID = W_ID;
        this.CARRIER_ID = CARRIER_ID;
    }

    public void execute() {
        retrieveAndUpdateOldestUndeliveredOrder();
        updateOrderline();
        updateCustomer();

        // For this transaction, it does not need to print results
    }

    private void retrieveAndUpdateOldestUndeliveredOrder() {

    }

    private void updateOrderline() {

    }

    private void updateCustomer() {
        
    }
}
