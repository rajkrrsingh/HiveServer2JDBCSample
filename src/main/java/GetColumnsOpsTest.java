import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetColumnsOpsTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = null;
        try {
            con = DriverManager.getConnection(args[0], args[1], args[2]);
        } catch (SQLException e) {
            e.printStackTrace();
        }

            try {
                ResultSet res = con.getMetaData().getColumns("", "", "%", "%");
                int cc = res.getMetaData().getColumnCount();
                while (res.next()) {
                    for (int i = 1; i <= cc; i++) {
                        System.out.print(res.getString(i) + ",");
                    }
                    System.out.println("");
                }
            } catch (java.sql.SQLException sqe) {
                System.out.println(sqe.getMessage());

            }

    }

}
