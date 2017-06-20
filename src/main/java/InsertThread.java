import java.sql.Connection;
import java.sql.Statement;

/**
 * Created by rasingh on 5/29/17.
 */
public class InsertThread implements  Runnable {

    Connection connection = null;
    String query = null;

    public InsertThread(Connection con,String query){
        this.connection = con;
        this.query = query;
    }

    @Override
    public void run() {
        System.out.println("starting thread "+Thread.currentThread().getName());
        runInsert();
        System.out.println(Thread.currentThread().getName()+" end");
    }

    public void runInsert() {
        try {
            Statement stmt = connection.createStatement();
            stmt.execute("set tez.queue.name=llap");

                    stmt.execute("set tez.am.resource.memory.mb=1024");
            stmt.execute("set hive.tez.container.size=512");
            System.out.println("Running: insert query; "+this.query);
            stmt.execute(this.query);
            stmt.close();
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
