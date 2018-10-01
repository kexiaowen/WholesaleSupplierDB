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
        String q1 = String.format(
                "Select * from Customer_T2_Print Where C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
                C_W_ID, C_D_ID, C_ID
        );
        Row row1 = session.execute(q1).one();
//        Customer’s identifier (C W ID, C D ID, C ID), name (C FIRST, C MIDDLE, C LAST), address
//                (C STREET 1, C STREET 2, C CITY, C STATE, C ZIP), C PHONE, C SINCE, C CREDIT,
//                C CREDIT LIM, C DISCOUNT, C BALANCE
        System.out.printf("Id: %s, %s, %s\nName: %s %s %s\nAddress: %s, %s, %s, %s, %s\n"
                + "Phone: %s\nSince: %s\nCredit: %s\nCredit limit: %lf\nDiscount: %lf\nBalance: %lf\n",
                row1.getString("C_W_ID"), row1.getString("C_D_ID"), row1.getString("C_ID"),
                row1.getString("C_FIRST"), row1.getString("C_MIDDLE"), row1.getString("C_LAST"),
                row1.getString("C_STREET_1"), row1.getString("C_STREET_2"), row1.getString("C_CITY"),
                row1.getString("C_STATE"), row1.getString("C_ZIP"),
                row1.getString("C_PHONE"), row1.getString("C_SINCE"), row1.getString("C_CREDIT"),
                row1.getDecimal("C_CREDIT_LIM"), row1.getDecimal("C_DISCOUNT"),
                row1.getDecimal("C_Balance"));

        String q2 = String.format(
                "Select * from Warehouse_T2_Print WHERE W_ID = %d;", C_W_ID
        );
        Row row2 = session.execute(q2).one();
        //Warehouse’s address (W STREET 1, W STREET 2, W CITY, W STATE, W ZIP)
        System.out.printf("Warehouse address: %s, %s, %s, %s, %s\n",
                row2.getString("W_STREET_1"), row2.getString("W_STREET_2"),
                row2.getString("W_CITY"), row2.getString("W_STATE"), row2.getString("W_ZIP"));

        String q3 = String.format(
                "Select * from District_T2_Print D_W_ID = %d and D_ID = %d;", C_W_ID, C_D_ID
        );
        Row row3 = session.execute(q3).one();
        //District’s address (D STREET 1, D STREET 2, D CITY, D STATE, D ZIP)
        System.out.printf("District address: %s, %s, %s, %s, %s\n",
                row3.getString("D_STREET_1"), row3.getString("D_STREET_2"),
                row3.getString("D_CITY"), row3.getString("D_STATE"), row3.getString("D_ZIP"));
        
        System.out.println("Payment: " + payment);
    }

    private void updateCustomer() {
        String q1 = String.format(
                "Select C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT from Customer_T2_update "
                + "Where C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
                C_W_ID, C_D_ID, C_ID
        );
        Row row1 = session.execute(q1).one();
        int balance = row1.getInt("C_BALANCE") - payment;
        int ytd = row1.getInt("C_YTD_PAYMENT") + payment;
        int cnt = row1.getInt("C_PAYMENT_CNT") + 1;
        String q2 = String.format(
                "Update Customer set C_BALANCE = %d, C_PAYMENT_CNT = %d, C_ID = %d "
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
