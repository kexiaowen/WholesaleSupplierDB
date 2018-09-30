import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Transaction2 {

    private String C_W_ID, C_D_ID, C_ID;
    private Session session;
    private int payment;

    public Transaction2(Session session, String C_W_ID, String C_D_ID, String C_ID,
                        int payment) {
        this.session = session;
        this.C_D_ID = C_D_ID;
        this.C_ID = C_ID;
        this.C_W_ID = C_W_ID;
        this.payment = payment;
    }

    public void execute() {
        update_W_YTD();
        update_D_YTD();
        updateCustomer();
        printResult();
    }

    private void printResult() {
        // print other info
        System.out.println(payment);
    }

    private void updateCustomer() {
        String q1 = String.format(
                "Select C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT from Customer "
                + "Where C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
                C_W_ID, C_D_ID, C_ID
        );
        Row row1 = session.execute(q1).one();
        int balance = row1.getInt("C_BALANCE") - payment;
        int ytd = row1.getInt("C_YTD_PAYMENT") + payment;
        int cnt = row1.getInt("C_PAYMENT_CNT") + 1;
        String q2 = String.format(
                "Update Customer set C_BALANCE = %d, C_PAYMENT_CNT = %d, C_ID = %d"
                + "Where C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
                balance, ytd, cnt, C_W_ID, C_D_ID, C_ID
        );
    }

    private void update_D_YTD() {
        String q1 = String.format(
                "SELECT D_YTD FROM District_T2 WHERE D_W_ID = %d and D_ID = %d;", C_W_ID, C_D_ID
        );
        Row row1 = session.execute(q1).one();
        int ytd = row1.getInt("D_YTD") + payment;
        String q2 = String.format(
                "UPDATE District SET D_YTD = %d WHERE D_W_ID = %d and D_ID = %d;",
                ytd, C_W_ID, C_D_ID
        );
        session.execute(q2);
    }

    private void update_W_YTD() {
        String q1 = String.format(
                "SELECT W_YTD FROM Warehouse_T2 WHERE W_ID = %d;", C_W_ID
        );
        Row row1 = session.execute(q1).one();
        int curYTD = Integer.valueOf(row1.getString("W_YTD"));
        String q2 = String.format(
                "UPDATE Warehouse SET W_YTD = %d WHERE W_ID = %d;", curYTD + payment, C_W_ID
        );
        session.execute(q2);
    }
}
