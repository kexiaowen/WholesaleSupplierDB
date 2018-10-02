import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Transaction4 {

    private int C_W_ID, C_D_ID, C_ID;
    private Session session;

    public Transaction4(Session session, int C_W_ID, int C_D_ID, int C_ID) {
        this.session = session;
        this.C_W_ID = C_W_ID;
        this.C_D_ID = C_D_ID;
        this.C_ID = C_ID;
    }

    public void execution() {
        String q1 = String.format(
                "SELECT C_FIRST, C_LAST, C_MIDDLE, C_BALANCE FROM Customer WHERE " +
                        "C_W_ID = %d AND C_D_ID = %d AND C_ID = %d;",
                C_W_ID, C_D_ID, C_ID
        );
        Row row1 = session.execute(q1).one();
        System.out.printf("Name: %s %s %s. Balance: %f",
                row1.getString("C_LAST"), row1.getString("C_MIDDLE"), row1.getString("C_FIRST"),
                row1.getDouble("C_BALANCE"));
    }
}
