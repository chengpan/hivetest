import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class Hive2JdbcClient {
	
		private static String columnsStr;
		private static String regexStr;
		private static boolean initFailed = true;
		
        static void getColumnsRegex(int id)
        {
				final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
				final String DB_URL = "jdbc:mysql://10.9.170.241:3306/ucdn";
				final String USER = "root";
				final String PASS = "ucdnred@cat;;";
				
				Connection conn = null;
				Statement stmt = null;
				try{
					// 注册 JDBC 驱动
					Class.forName(JDBC_DRIVER);
					
					conn = DriverManager.getConnection(DB_URL, USER, PASS);
					stmt = conn.createStatement();
					String sql;
					sql = String.format("select * from tb_log_analyze_conf where id = %d", id);
					ResultSet rs = stmt.executeQuery(sql);
					
					if (rs.next()){
						columnsStr = rs.getString("columns");
						regexStr = rs.getString("regex");
						initFailed = false;
					}
					// 完成后关闭
					rs.close();
					stmt.close();
					conn.close();
					
				}catch(SQLException se){
					// 处理 JDBC 错误
					se.printStackTrace();
				}catch(Exception e){
					// 处理 Class.forName 错误
					e.printStackTrace();
				}finally{
					// 关闭资源
					try{
						if(stmt != null) stmt.close();
					}catch(SQLException se2){
					}// 什么都不做
					try{
						if(conn != null) conn.close();
					}catch(SQLException se){
						se.printStackTrace();
					}
				}
        }

        /**
         * @param args
         * @throws SQLException
         */
        public static void main(String[] args) throws SQLException {
				//初始化
				int mysqlId = 10000;
				getColumnsRegex(mysqlId);
				if (initFailed)
				{
					System.out.println("init failed !");
					System.exit(1);
				}
				System.out.println("columnsStr: " + columnsStr);
				System.out.println("regexStr: " + regexStr);
			
				String driverName = "org.apache.hive.jdbc.HiveDriver";
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

                String tableColumns = "time string" +
								      ", clientIp string" +
									  ", method string" +
									  ", path string" +
									  ", query string" +
									  ", http_ver string" +
									  ", hit_status int" +
									  ", http_status int" +
									  ", bytes_with_hdr bigint" +
									  ", resp_delay int" +
									  ", host_name string" +
									  ", referer string" +
									  ", user_name string" +
									  ", back2src_method string" +
									  ", back2src_host string" +
									  ", server_ip string" +
									  ", source_ip string" +
									  ", source_ret_code string" + //warning!!! not int
									  ", cli_req_end string" + //warning!!! not int
									  ", src_req_end string" + //warning!!! not int
									  ", user_agent string" +
									  ", req_start_time int" +
									  ", front_flow int" +
									  ", range string" +
									  ", bytes_no_hdr bigint" +
									  ", file_bytes bigint" +
									  ", cache_bytes bigint" +
									  ", src_bytes bigint" +
									  ", internal_err int" +
									  ", finish_status int" +
									  ", ufile_bytes bigint";
                tableColumns = tableColumns + ", trail string";//trail
				String regexStr = "[\\\\s]*\\\\[([0-9-: ]+)\\\\]" + //time
                       "[\\\\s]+([0-9.]+)" + //client ip
                       "[\\\\s]+\\\"([^ ]+) ([^ ?]+)\\\\??([^ ]+)? ([^\\\"]+)\\\"" + // method + path + query + version
                       "[\\\\s]+([^\\\\s]+)" + //hit status
                       "[\\\\s]+([^\\\\s]+)" + //http status
                       "[\\\\s]+([^\\\\s]+)" + //length
                       "[\\\\s]+([^\\\\s]+)" + //response delay
                       "[\\\\s]+([^\\\\s]+)" + //host
                       "[\\\\s]+\\\"([^\\\"]*)\\\"" + //referer
                       "[\\\\s]+([^\\\\s]+)" + //user name
                       "[\\\\s]+([^/]+)/([^\\\\s]+)" + //source back method/source host 回源方式/域名
                       "[\\\\s]+([^\\\\s]+)" + //server ip
                       "[\\\\s]+([^\\\\s]+)" + //source ip
                       "[\\\\s]+([^\\\\s]+)" + //source return code
                       "[\\\\s]+([^\\\\s]+)" + //client req end mark 客户端请求结束标记
                       "[\\\\s]+([^\\\\s]+)" + //source req end mark 源端请求结束标记
                       "[\\\\s]+\\\"([^\\\"]*)\\\"" + //user agent
                       "[\\\\s]+([^\\\\s]+)" + //request start time
                       "[\\\\s]+([^\\\\s]+)" + //front end flow 前端flow
                       "[\\\\s]+([^\\\\s]+)" + //range
                       "[\\\\s]+([^\\\\s]+)" + //bytes sent (without http header)
                       "[\\\\s]+([^\\\\s]+)" + //file bytes
                       "[\\\\s]+([^\\\\s]+)" + //from cache bytes
                       "[\\\\s]+([^\\\\s]+)" + //from source bytes
                       "[\\\\s]+([^\\\\s]+)" + //internal error
                       "[\\\\s]+([^\\\\s]+)" + //finish status
                       "[\\\\s]+([^\\\\s]+)";  //from ufile size
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
