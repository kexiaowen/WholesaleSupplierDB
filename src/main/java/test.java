import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

public class test {
    ArrayList<Integer> a = new ArrayList<Integer>();
    ArrayList<Integer> b = new ArrayList<Integer>();

    public static void main(String[] args) {
        new test().run();

    }

    private void run() {
        for (int i = 0; i < 5; i++) {
            a.add(i);
            b.add(i + 5);
        }
        Iterator<Integer> ite1 = a.iterator();
        while (ite1.hasNext()) {
            int n1 = ite1.next();
            Iterator<Integer> ite2 = b.iterator();
            while (ite2.hasNext()) {
                int n2 = ite2.next();
                System.out.printf("n1: %d, n2: %d\n", n1, n2);
            }
        }
    }
}
