import org.apache.log4j.Logger;

import java.sql.*;
public class HiveJdbcClient {
  final static Logger logger = Logger.getLogger(HiveJdbcClient.class);
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
    if(args.length <3){
      System.err.println("java -jar HiveServer2JDBCTest-jar-with-dependencies.jar <connection_string> <userid> <password>");
      System.exit(1);
    }
      try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }
    Connection con=null;
    Statement stmt=null;
    ResultSet res=null;
    try {
      con = DriverManager.getConnection(args[0], args[1], args[2]);
      stmt = con.createStatement();
      System.out.println("Running: show databases; ");
      res = stmt.executeQuery("show databases");
      while (res.next()) {
        System.out.println(res.getString(1));
      }
    }catch (Exception e){
      e.printStackTrace();
    }finally{
      res.close();
      stmt.close();
      con.close();
    }

  }
}