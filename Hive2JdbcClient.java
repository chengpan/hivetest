import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class Hive2JdbcClient {
        private static String driverName = "org.apache.hive.jdbc.HiveDriver";
        static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        static final String DB_URL = "jdbc:mysql://localhost:3306/RUNOOB";
        static String[] getColumnsRegex()
        {
                String[] arrStr = new String[2];
                arrStr[0] = "columns";
                arrStr[1] = "regex";
                return arrStr;
        }

        /**
         * @param args
         * @throws SQLException
         */
        public static void main(String[] args) throws SQLException {
                try {
                        Class.forName(driverName);
                } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        System.exit(1);
                }

                String[] colReg = getColumnsRegex();
                //replace "hive" here with the name of the user the queries should run as
                Connection con = DriverManager.getConnection("jdbc:hive2://10.9.96.4:10000/default", "root", "ucdnred@cat;;");
                Statement stmt = con.createStatement();
                String tableName = "bigfile";
                stmt.execute("drop table if exists " + tableName);



                String logPath = "/hdtest/20161129/auc.tangdouimg.com/";

                String tableColumns = "time string, clientIp string, method string, path string, query string, http_ver string";
                tableColumns = tableColumns + ", trail string";//trail

                String regexStr = "\\\\[([0-9-: ]+)\\\\][\\\\s]+([0-9.]+)[\\\\s]+\\\"([^ ]+) ([^ ?]+)\\\\??([^ ]+)? ([^\\\"]+)\\\"";
                regexStr = regexStr + "([\\\\s]+.*)?";//trail

                String tableStr = String.format("create external table if not exists %s(%s)", tableName, tableColumns);
                String serdeStr = String.format("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' WITH SERDEPROPERTIES ('input.regex' = '%s')", regexStr);
                String locationStr = String.format("location '%s'", logPath);
                String queryStr = tableStr + "\n" + serdeStr + "\n" + locationStr;
                System.out.println(queryStr);
                stmt.execute(queryStr);
                // show tables
                String sql = "show tables '" + tableName + "'";
                System.out.println("Running: " + sql);
                ResultSet res = stmt.executeQuery(sql);
                if (res.next()) {
                        System.out.println(res.getString(1));
                }
                // show user
                sql = "select current_user() ";
                System.out.println("Running: " + sql);
                res = stmt.executeQuery(sql);
                while (res.next()) {
                        System.out.println(res.getString(1)); 
                }
                // describe table
                sql = "describe " + tableName;
                System.out.println("Running: " + sql);
                res = stmt.executeQuery(sql);
                while (res.next()) {
                        System.out.println(res.getString(1) + "\t" + res.getString(2)); 
                }


                // select * query
                sql = "select * from " + tableName + " limit 10";
                System.out.println("Running: " + sql);
                res = stmt.executeQuery(sql);
                //System.out.printf("%-20s, %-20s\n", "time", "clientip");
                while (res.next()) {
                        //System.out.println(String.valueOf(res.getString("time")) + "\t" + res.getString("clientip"));
                        System.out.printf("time          : %s\n", res.getString("time"));
                        System.out.printf("clientip      : %s\n", res.getString("clientip"));
                }

                // regular hive query
                sql = "select count(*) from " + tableName;
                System.out.println("Running: " + sql);
                res = stmt.executeQuery(sql);
                while (res.next()) {
                        System.out.println(res.getString(1));
                }
        }
}
