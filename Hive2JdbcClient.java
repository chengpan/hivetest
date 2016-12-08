import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;

class DomainConf{
	String domainName;
	int fileTpye;
	String columns;
	String regex;
	boolean initFailed;
	
	public DomainConf(String domainName, int fileTpye)
	{
		this.domainName = domainName;
		this.fileTpye = fileTpye;
		initFailed = true;
		getColumnsRegex();
	}
	
	private void getColumnsRegex()
	{
		final String DB_URL = "jdbc:mysql://10.9.170.241:3306/ucdn";
		final String USER = "root";
		final String PASS = "ucdnred@cat;;";
		Connection conn = null;
		Statement stmt = null;
		try{
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement();
			String sql;
			sql = String.format("select * from tb_log_analyze_conf" +
								" where file_type = %d and domain_name in ('%s', 'default')" +
								" order by id desc limit 1", //不存在的时候自动使用默认值
								 fileTpye,
								 domainName.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'"));
			System.out.println("sql to get conf: " + sql);
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()){
				columns = rs.getString("columns");
				regex = rs.getString("regex");
				System.out.println("using conf for: " + rs.getString("domain_name"));
				System.out.println("got columns: " + columns);
				System.out.println("got regex: " + regex);
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
}

class RunTest{
	String domainName;
	int fileTpye;
	String logPath;

	public RunTest(String domainName, int fileTpye, String logPath)
	{
		this.domainName = domainName;
		this.fileTpye = fileTpye;
		this.logPath = logPath;
	}

	public void run()
	{
            DomainConf dConf = new DomainConf(domainName, fileTpye);
            if(dConf.initFailed)
            {
            	System.out.println("get conf from mysql failed !");
            	return;
            }

            Connection con = null;
            Statement stmt = null;
            ResultSet res  = null;
            try
            {
                con = DriverManager.getConnection("jdbc:hive2://10.9.96.4:10000/default", "root", "ucdnred@cat;;");
                stmt = con.createStatement();

                String curTimeStr = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
                System.out.println("curTimeStr: " + curTimeStr);

                String tableName = "temp" + curTimeStr + dConf.domainName.replaceAll("\\W", "_") + "_" + dConf.fileTpye;
                System.out.println("tableName: " + tableName);

                stmt.execute("drop table if exists " + tableName);

                String tableStr = String.format("create external table if not exists %s(%s)",
                								 tableName, dConf.columns);
                String serdeStr = String.format("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'" +
                								" WITH SERDEPROPERTIES ('input.regex' = '%s')", dConf.regex);
                String locationStr = String.format("location '%s'", logPath);
                String queryStr = tableStr + "\n" + serdeStr + "\n" + locationStr;
                System.out.println(queryStr);
                stmt.execute(queryStr);
                // show tables
                String sql = "show tables '" + tableName + "'";
                System.out.println("Running: " + sql);
               	res = stmt.executeQuery(sql);
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
                sql = "select * from " + tableName + " limit 1";
                System.out.println("Running: " + sql);
                res = stmt.executeQuery(sql);
                while (res.next()) {
                        System.out.printf("time              : %s\n", res.getString("time"));
                        System.out.printf("clientip          : %s\n", res.getString("clientip"));
                        System.out.printf("path              : %s\n", res.getString("path"));
                        System.out.printf("query             : %s\n", res.getString("query"));
                        System.out.printf("http_ver          : %s\n", res.getString("http_ver"));
                        System.out.printf("hit_status        : %s\n", res.getString("hit_status"));
                        System.out.printf("http_status       : %d\n", res.getInt("http_status"));
                        System.out.printf("bytes_with_hdr    : %s\n", res.getString("bytes_with_hdr"));
                        System.out.printf("resp_delay        : %d\n", res.getInt("resp_delay"));
                        System.out.printf("host_name         : %s\n", res.getString("host_name"));
                        System.out.printf("referer           : %s\n", res.getString("referer"));
                        System.out.printf("back2src_method   : %s\n", res.getString("back2src_method"));
                        System.out.printf("back2src_host     : %s\n", res.getString("back2src_host"));
                        System.out.printf("server_ip         : %s\n", res.getString("server_ip"));
                        System.out.printf("source_ip         : %s\n", res.getString("source_ip"));
                        System.out.printf("source_ret_code   : %s\n", res.getString("source_ret_code"));
                        System.out.printf("cli_req_end       : %s\n", res.getString("cli_req_end"));
                        System.out.printf("src_req_end       : %s\n", res.getString("src_req_end"));
                        System.out.printf("user_agent        : %s\n", res.getString("user_agent"));
                }

                // regular hive query
                sql = "select count(*) as total_lines from " + tableName;
                System.out.println("Running: " + sql);
                res = stmt.executeQuery(sql);
                while (res.next()) {
                        System.out.println("total_lines: " + res.getLong("total_lines"));
                }

                sql = "select count(*) as null_lines from " + tableName + " where time is null";
                System.out.println("Running: " + sql);
                res = stmt.executeQuery(sql);
                while (res.next()) {
                        System.out.println("null_lines: " + res.getLong("null_lines"));
                }

                //删除表
                stmt.execute("drop table if exists " + tableName);
	            
	            // 完成后关闭
				res.close();
				stmt.close();
				con.close();
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
					if(con != null) con.close();
				}catch(SQLException se){
					se.printStackTrace();
				}
			}	
	}
}

public class Hive2JdbcClient {

        public static void main(String[] args){

        		// 注册 HIVE 驱动
				String driverName = "org.apache.hive.jdbc.HiveDriver";
                try {
					Class.forName(driverName);
                } catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(1);
                }

                // 注册 JDBC 驱动
				driverName = "com.mysql.jdbc.Driver";
                try {
					Class.forName(driverName);
                } catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(1);
                }

                RunTest testSmall = new RunTest("dh5.kimg.cn", 0, "/logs/20161207/dh5.kimg.cn");
                testSmall.run();

                RunTest testBig = new RunTest("auc.tangdou.com", 1, "/logs/20161207/auc.tangdou.com");
                testBig.run();               

        }
}
