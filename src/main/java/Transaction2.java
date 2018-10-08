import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Transaction2 {

    private int C_W_ID, C_D_ID, C_ID;
    private Session session;
    private double payment;

    public Transaction2(Session session, int C_W_ID, int C_D_ID, int C_ID,
                        double payment) {
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
        String q7 = String.format(
                "Select C_FIRST, C_MIDDLE, C_LAST, " +
                        "C_STREET_1, C_STREET_2, C_CITY, " +
                        "C_STATE, C_ZIP, C_PHONE, " +
                        "C_SINCE, C_CREDIT, C_CREDIT_LIM, " +
                        "C_DISCOUNT, C_Balance " +
                        "FROM Customer Where C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
                C_W_ID, C_D_ID, C_ID
        );
        Row row7 = session.execute(q7).one();
        System.out.printf("Id: %d, %d, %d\nName: %s %s %s\nAddress: %s, %s, %s, %s, %s\n"
                + "Phone: %s\nSince: %s\nCredit: %s\nCredit limit: %f\nDiscount: %f\nBalance: %f\n",
                C_W_ID, C_D_ID, C_ID,
                row7.getString("C_FIRST"), row7.getString("C_MIDDLE"), row7.getString("C_LAST"),
                row7.getString("C_STREET_1"), row7.getString("C_STREET_2"), row7.getString("C_CITY"),
                row7.getString("C_STATE"), row7.getString("C_ZIP"),
                row7.getString("C_PHONE"), row7.getTimestamp("C_SINCE").toString(), row7.getString("C_CREDIT"),
                row7.getDecimal("C_CREDIT_LIM").doubleValue(), row7.getDecimal("C_DISCOUNT").doubleValue(),
                row7.getDecimal("C_Balance").doubleValue());

        String q8 = String.format(
                "Select W_STREET_1, W_STREET_2, " +
                        "W_CITY, W_STATE, W_ZIP from Warehouse WHERE W_ID = %d;", C_W_ID
        );
        Row row8 = session.execute(q8).one();
        System.out.printf("Warehouse address: %s, %s, %s, %s, %s\n",
                row8.getString("W_STREET_1"), row8.getString("W_STREET_2"),
                row8.getString("W_CITY"), row8.getString("W_STATE"), row8.getString("W_ZIP"));

        String q9 = String.format(
                "Select D_STREET_1, D_STREET_2, D_CITY, D_STATE, " +
                        "D_ZIP from District WHERE D_W_ID = %d and D_ID = %d;", C_W_ID, C_D_ID
        );
        Row row9 = session.execute(q9).one();
        System.out.printf("District address: %s, %s, %s, %s, %s\n",
                row9.getString("D_STREET_1"), row9.getString("D_STREET_2"),
                row9.getString("D_CITY"), row9.getString("D_STATE"), row9.getString("D_ZIP"));
        
        System.out.println("Payment: " + payment);
    }

    private void updateCustomer() {
        String q5 = String.format(
                "Select C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT from Customer "
                + "Where C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
                C_W_ID, C_D_ID, C_ID
        );
        Row row5 = session.execute(q5).one();
        double balance = row5.getDecimal("C_BALANCE").doubleValue() - payment;
        double ytd = row5.getFloat("C_YTD_PAYMENT") + payment;
        int cnt = row5.getInt("C_PAYMENT_CNT") + 1;
        String q6 = String.format(
                "Update Customer set C_BALANCE = %f, C_YTD_PAYMENT = %f, C_PAYMENT_CNT = %d "
                + "Where C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
                balance, ytd, cnt, C_W_ID, C_D_ID, C_ID
        );
        session.execute(q6);
    }

    private void update_D_YTD() {
        String q3 = String.format(
                "SELECT D_YTD FROM District WHERE D_W_ID = %d and D_ID = %d;", C_W_ID, C_D_ID
        );
        Row row3 = session.execute(q3).one();
        double ytd = row3.getDecimal("D_YTD").doubleValue() + payment;
        String q4 = String.format(
                "UPDATE District SET D_YTD = %f WHERE D_W_ID = %d and D_ID = %d;",
                ytd, C_W_ID, C_D_ID
        );
        session.execute(q4);
    }

    private void update_W_YTD() {
        String q1 = String.format(
                "SELECT W_YTD FROM Warehouse WHERE W_ID = %d;", C_W_ID
        );
        Row row1 = session.execute(q1).one();
        double ytd = row1.getDecimal("W_YTD").doubleValue();
        String q2 = String.format(
                "UPDATE Warehouse SET W_YTD = %f WHERE W_ID = %d;", ytd + payment, C_W_ID
        );
        session.execute(q2);
    }
}
