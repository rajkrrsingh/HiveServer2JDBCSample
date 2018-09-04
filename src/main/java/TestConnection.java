import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class TestConnection {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		try {
			Connection con = DriverManager.getConnection(
					"jdbc:hive2://hdp263c.hdp.local:2181,hdp263b.hdp.local:2181,hdp263a.hdp.local:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2",
					"ambari-qa", "password");
			long startTime = System.nanoTime();
			ResultSet res = con.getMetaData().getTables("", "", "%", new String[] { "TABLE", "VIEW" });
			long endTime = System.nanoTime();
			System.out.println("Time Taken : "+(endTime - startTime)/1000000+"ms");
			//int cc = res.getMetaData().getColumnCount();
			//System.out.println("writing tables name");
			int i=0;
			while (res.next()) {
				i++;
			}
			System.out.println("res returned"+i);
		} catch (java.sql.SQLException sqe) {
			sqe.printStackTrace(System.err);
		}
	}

}
