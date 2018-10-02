import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Transaction1 {
    private Session session;
    private int W_ID;
    private int D_ID;
    private int C_ID;
    private int num_items;
    private int[] item_number;
    private int[] supplier_warehouse;
    private int[] quantity;
    private String[] itemName;
    private double[] itemPrice;
    private int[] stockQuantity;

    public Transaction1(Session session, int W_ID, int D_ID, int C_ID, int num_items,
                        int[] item_number, int[] supplier_warehouse, int[] quantity) {
        this.session = session;
        this.W_ID = W_ID;
        this.D_ID = D_ID;
        this.C_ID = C_ID;
        this.num_items = num_items;
        this.item_number = item_number;
        this.supplier_warehouse = supplier_warehouse;
        this.quantity = quantity;
        itemName = new String[num_items];
        itemPrice = new double[num_items];
        stockQuantity = new int[num_items];
    }

    private int retrieveAndUpdateOID() {
        String q1 = String.format(
                "SELECT D_NEXT_O_ID FROM District WHERE D_W_ID = %d AND D_ID = %d;",
                W_ID, D_ID);

        Row row1 = session.execute(q1).one();
        int nextOID = Integer.valueOf(row1.getString("D_NEXT_O_ID"));

        // sout
        //System.out.println("nextOID = " + nextOID);
        String q2 = String.format(
                "UPDATE District SET D_NEXT_ID = %d WHERE D_W_ID = %d AND D_ID = %d;",
                nextOID + 1, W_ID, D_ID
        );
        session.execute(q2);
        return nextOID;
    }

    private void insertNewOrder(int nextOID) {
        int all_local = 1;
        for (int i = 0; i < num_items; i++) {
            if (supplier_warehouse[i] != W_ID) {
                all_local = 0;
                break;
            }
        }

        String q3 = String.format(
                "INSERT INTO Orders (O_W_ID, O_D_ID, O_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) "
                        + "VALUES (%d, %d, %d, %d, %s, %d, %d, %d);",
                W_ID, D_ID, nextOID, C_ID, "toTimeStamp(now())", -1, num_items, all_local
        );
        session.execute(q3);
    }

    private double insertOrderLinesAndComputePrice(int O_ID) {
        double total_amount = 0;

        for (int i = 0; i < num_items; i++) {
            String q4 = String.format(
                    "SELECT * FROM Stock WHERE S_W_ID = %d AND S_I_ID = %d;",
                    supplier_warehouse[i], item_number[i]);
            Row row4 = session.execute(q4).one();
            int adjustedQuantity = row4.getInt("S_QUANTITY") - quantity[i];
            int ytd = row4.getInt("S_YTD") + quantity[i];
            int cnt = row4.getInt("S_ORDER_CNT") + 1;
            int remoteCnt = row4.getInt("S_REMOTE_CNT");
            String distInfo = row4.getString("S_DIST_" + D_ID);
            if (supplier_warehouse[i] != W_ID) {
                remoteCnt++;
            }
            if(adjustedQuantity < 10) {
                adjustedQuantity += 100;
            }
            stockQuantity[i] = adjustedQuantity;
            String q5 = String.format(
                    "UPDATE Stock SET S_QUANTITY = %d, S_YTD = %d, S_ORDER_CNT = %d, S_REMOTE_CNT = %d"
                            + "WHERE S_W_ID = %d AND S_I_ID = %d;",
                    adjustedQuantity, ytd, cnt, remoteCnt, supplier_warehouse[i], item_number[i]
            );
            session.execute(q5);

            String q6 = String.format("SELECT I_PRICE, I_NAME FROM Item Where I_ID = %d", item_number[i]);
            Row row6 = session.execute(q6).one();
            itemPrice[i] = row6.getDouble("I_PRICE");
            itemName[i] = row6.getString("I_NAME");
            double itemAmount = quantity[i] * itemPrice[i];
            total_amount += itemAmount;

            String q7 = String.format(
                    "INSERT INTO OrderLine (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D,"
                            + " OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO)"
                            + "VALUES (%d, %d, %d, %d, %d, %d, %f, %d, %d, %s)",
                    W_ID, D_ID, O_ID, i, item_number[i], -1,
                    itemAmount, supplier_warehouse[i], quantity[i], distInfo
            );
            session.execute(q7);
        }

        return total_amount;
    }

    private void printResult(int OID, double rawAmount) {
        // #1
        String q8 = String.format(
                "SELECT C_LAST, C_CREDIT, C_DISCOUNT FROM CUSTOMER"
                        + "WHERE C_W_ID = %d AND C_D_ID = %d AND = C_ID = %d;",
                W_ID, D_ID, C_ID
        );
        Row customerRow = session.execute(q8).one();
        double discount = customerRow.getDouble("C_DISCOUNT");
        System.out.printf("Customer identifier: %d, %d, %d, lastname: %s, credit: %s, discount: %f\n",
                W_ID, D_ID, C_ID, customerRow.getString("C_LAST"), customerRow.getString("C_CREDIT"),
                discount);

        // #2
        String q9 = String.format("SELECT W_TAX FROM Warehouse WHERE W_ID = %d;", W_ID);
        double wTax = session.execute(q9).one().getDouble("W_TAX");
        String q10 = String.format("SELECT D_TAX FORM District WHERE D_W_ID = %d AND D_ID = %d;", W_ID, D_ID);
        double dTax = session.execute(q10).one().getDouble("D_TAX");
        System.out.println("W_Tax: " + wTax + " D_Tax: " + dTax);

        // TODO: print entry date
        // #3
        System.out.println("O_ID: " + OID);

        // #4
        double totalAmount = rawAmount * (1 + wTax + dTax) * (1 - discount);
        System.out.println("Num_items: " + num_items + " Total_amount: " + totalAmount);

        // #5
        for (int i = 0; i < num_items; i++) {
            System.out.printf("Item number: %d, Item name: %s, Supplier warehouse: %d, Quantity: %d" +
                    "OL amount: %f, S_quantity: %d\n", item_number[i], itemName[i], supplier_warehouse[i],
                    quantity[i], itemPrice[i] * quantity[i], stockQuantity[i]);
        }
    }

    public void execute() {
        int nextOID = retrieveAndUpdateOID();
        insertNewOrder(nextOID);
        // End of last meeting
        double rawAmount = insertOrderLinesAndComputePrice(nextOID);
        printResult(nextOID, rawAmount);
    }

}
