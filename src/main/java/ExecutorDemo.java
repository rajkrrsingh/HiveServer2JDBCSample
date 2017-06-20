import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by rasingh on 5/29/17.
 */
public class ExecutorDemo {
    private static final Logger log = LoggerFactory.getLogger(ExecutorDemo.class);

    public static void main(String[] args) {
        ExecutorService  service = Executors.newFixedThreadPool(50);
        for (int i = 0; i < 50 ; i++) {
            String name = "name"+i;
            String city = "city"+i;
            String email = "email"+i;
            String query = "insert into table census_clus values ("+i+",'"+name+"','"+city+"','"+email+"')";
            Runnable runnable = new InsertThread(getHiveConnection(),query);
            service.execute(runnable);
        }
        service.shutdown();
        while (!service.isTerminated()) {
        }
        System.out.println("Finished all threads");

    }

    public static Connection getHiveConnection(){
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection con = null;
        try {
            con = DriverManager.getConnection("jdbc:hive2://llapp2.hdp.local:2181,llapp1.hdp.local:2181,llapp3.hdp.local:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2","hive","hive");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        log.info("[Thread: "+Thread.currentThread().getName()+"] | [method: "+Thread.currentThread().getStackTrace()[1].getMethodName()+" ] | "+con);
        return con;
    }
}
