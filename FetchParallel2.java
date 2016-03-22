import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

//This code will chunk the original query up to run in parallel with multiple threads.  

public class FetchParallel2 {

	private static BufferedWriter outputBuffer = null;

	private static int SIZEOFBUFFER = 150;
	Connection conn = null;
	Statement stmt = null;
	ResultSet rs = null;
	ResultSet rs_pkeys = null;
	int noOfThreads = 4;
	int start = 0, end = 0;
	static long startTime = 0;
	static long endTime = 0;
	String connection_str = "jdbc:vertica://myip:5433/vmart?user=dbadmin&password=dbadmin";
	String analyze_stats = "1";
	String print_only = "0";
	String load_bal = "0";

	public static void main(String[] args) throws SQLException, IOException {

		String outputfilename = "query_chunker_log_file.log";
		long startTime_job = 0;
		String original_sql = "select if.* from inventory_fact if, warehouse_dimension wd where if.warehouse_key = wd.warehouse_key";

		FileWriter outputFW = new FileWriter(outputfilename);
		outputBuffer = new BufferedWriter(outputFW, SIZEOFBUFFER);

		// TODO: Check number of nodes and send query to each node.
		// select set_load_balance_policy('ROUNDROBIN') ;
		// select set_load_balance_policy('NONE');
		// SELECT RESET_LOAD_BALANCE_POLICY();
		// SELECT LOAD_BALANCE_POLICY FROM V_CATALOG.DATABASES;
		// SELECT NODE_NAME FROM V_MONITOR.CURRENT_SESSION;
		// Do explain plan to see if parallel query should be done.
		// Check thread safe
		// also should have arguments to pull in connection string.
		// also create projections on sql.
		/*
		 * Should check data type and the length of the keys chosen. 
		 * Should not pick a varchar > n ?
		 * Should not pick other datatypes like varbinary?
		 * etc....
		 * Need to add invoke via vsql as an option...
		 * 
		 * 		 */
		if (args.length > 0) {
			original_sql = args[0];

		}

		System.out.println("Original SQL: " + original_sql);

		outputBuffer.write("Original SQL: " + original_sql);
		outputBuffer.newLine();

		String tableName = null;
		startTime = System.currentTimeMillis();
		startTime_job =System.currentTimeMillis();
		System.out.println("Started at: " + startTime + "\n\n");
		outputBuffer.write("Started at: " + startTime + "\n\n");
		outputBuffer.newLine();

		FetchParallel2 fetchParallel = new FetchParallel2();

		/*
		 * TODO: 1.) Find Table Names by finding table with largest number of
		 * rows and set tableName with that.
		 */
		String justtbls = fetchParallel.findTblNames(original_sql);

		int nbr_of_commans = justtbls.indexOf(",");

		Map<String, String> unsortMapTables = new HashMap<String, String>();
		String sql_count = null;
		String cnt = null;
		String[] table_name = null;
		String[] tables = null;
		

		
		if (nbr_of_commans > 0) {
			tables = justtbls.split(",");
			int numberoftbls = tables.length;

			System.out.println("Total nbr of tables in query:  " + numberoftbls);
			outputBuffer.write("Total nbr of tables in query:  " + numberoftbls);
			outputBuffer.newLine();

			for (int i = 0; i < tables.length; i++) {

				table_name = tables[i].trim().split(" ");
				sql_count = "select count(*) from " + table_name[0];
				System.out.println("Sql to get executed: " + sql_count);
				outputBuffer.write("Sql to get executed: " + sql_count);
				outputBuffer.newLine();

				cnt = fetchParallel.runSimpleSQL(sql_count);
				unsortMapTables.put(cnt, table_name[0]);
			}
			
			Map<String, String> treeMap = new TreeMap<String, String>(unsortMapTables);
			fetchParallel.printMap(treeMap);
			tableName = fetchParallel.getLast(treeMap, "value");
			
		} else {
			tables = justtbls.trim().split(" ");
			unsortMapTables.put(cnt, tables[0]);
			System.out.println("only one table in sql " + tables[0]);
			outputBuffer.write("only one table in sql " + tables[0]);
			outputBuffer.newLine();
			tableName = tables[0];
		}
		

		
		System.out.println("table name is  " + tableName);
		fetchParallel.fetchData(original_sql, tableName);

		Thread.currentThread().yield();

		endTime = System.currentTimeMillis();
		System.out.println("end at: " + endTime + "\n\n");
		outputBuffer.write("end at: " + endTime + "\n\n");
		outputBuffer.newLine();

		System.out.println("Total time taken: " + (endTime - startTime_job));
		outputBuffer.write("Total time taken: " + (endTime - startTime_job));
		outputBuffer.newLine();

		if (fetchParallel.print_only.equalsIgnoreCase("1"))
			fetchParallel.findOSConfig();

		if (outputBuffer != null)
			outputBuffer.close();

	}

	public void printMap(Map<String, String> map) throws IOException {
		for (Map.Entry entry : map.entrySet()) {
			System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
			outputBuffer.write("Key : " + entry.getKey() + " Value : " + entry.getValue());
			outputBuffer.newLine();
		}
	}

	public String getLast(Map<String, String> map, String valueorkey) {
		String result = " ";
		for (Map.Entry entry : map.entrySet()) {
			if (valueorkey.equalsIgnoreCase("value")) {
				result = entry.getValue().toString();
			} else {
				result = entry.getKey().toString();
			}
		}

		return result;
	}

	public String getMapKey(Map<String, String> map) {
		String key = " ";
		for (Map.Entry entry : map.entrySet()) {
			// System.out.println("Key : " + entry.getKey() + " Value : " +
			// entry.getValue());
			key = key + entry.getValue().toString() + ",";
		}
		key = key.substring(0, key.length() - 1);
		return key;
	}

	protected Map<String, String> getChunkKeys(ResultSet rs) {

		StringBuffer sb = new StringBuffer();
		Map<String, String> unsortMap = new HashMap<String, String>();

		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int nCols = rsmd.getColumnCount();

			for (int i = 1; i <= nCols; i++) {
				if (i != 1)
					sb.append(", ");
				sb.append(rsmd.getColumnLabel(i));
			}

			// print column headers to stdout
			// System.out.println(sb.toString());

			// print results to stdout
			String col = null;
			String value = null;
			while (rs.next()) {
				// use clear() or trim() when they are available for
				// StringBuffers
				StringBuffer sb1 = new StringBuffer();
				for (int i = 1; i <= nCols; i++) {
					if (i != 1) {
						sb1.append(", ");
						value = rs.getObject(i).toString();
					} else {
						col = rs.getObject(i).toString();
					}
					sb1.append(rs.getObject(i));
				}

				unsortMap.put(value, col);

			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
		Map<String, String> treeMap = new TreeMap<String, String>(unsortMap);
		return treeMap;
	}

	protected void printResults2(ResultSet rs) throws IOException {
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int nCols = rsmd.getColumnCount();

			StringBuffer sb = new StringBuffer();

			for (int i = 1; i <= nCols; i++) {
				if (i != 1)
					sb.append(", ");
				sb.append(rsmd.getColumnLabel(i));
			}

			// print column headers to stdout
			System.out.println(sb.toString());
			outputBuffer.write((sb.toString()));
			outputBuffer.newLine();

			// print results to stdout
			while (rs.next()) {
				// use clear() or trim() when they are available for
				// StringBuffers
				StringBuffer sb1 = new StringBuffer();
				for (int i = 1; i <= nCols; i++) {
					if (i != 1)
						sb1.append(", ");
					sb1.append(rs.getObject(i));
				}

				System.out.println(sb1.toString());
				outputBuffer.write((sb1.toString()));
				outputBuffer.newLine();
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public String findTblNames(String sql) {
		/*
		 * This code will only work for simple sql cases. This code should use a
		 * sql parser to get the table names...
		 */
		String sqlLower = sql.toLowerCase();
		int lenghtofsql = sql.length();

		int fromstart = sql.indexOf("from");
		int wherestart = sql.indexOf("where");
		if (wherestart == 0) {
			wherestart = lenghtofsql;
		}
		// int fromtowhere = wherestart - fromstart;
		// String fromtowherestring = sqlLower.substring(fromstart, wherestart);
		String justtbls = sqlLower.substring(fromstart + 5, wherestart - 1);
		// String justtbls = fromtowherestring = sqlLower.substring(fromstart +
		// 5, wherestart - 1);

		return justtbls;

	}

	public String runSimpleSQL(String sql) throws SQLException {

		conn = DriverManager.getConnection(connection_str);
		stmt = conn.createStatement();
		String results = " ";
		StringBuffer sb1 = null;
		rs = stmt.executeQuery(sql);
		handleWarnings(rs.getWarnings());

		while (rs.next()) {
			// use clear() or trim() when they are available for
			// StringBuffers
			sb1 = new StringBuffer();

			sb1.append(rs.getObject(1));
		}

		results = sb1.toString();
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }

		return results;
	}

	public String findChunkCols(String orginal_SQL_predicates, String tableName) throws SQLException, IOException {

		conn = DriverManager.getConnection(connection_str);
		stmt = conn.createStatement();
		String primaryKey = " ";

		// This sql will only return results if the analyze_statistics have been
		// run.

		String sql_chunks = "select pc.name,  sum(distinct_count) from  vs_projections p , vs_projection_columns pc, vs_projection_column_histogram pch where anchortablename ='"
				+ tableName
				+ "' and pc.name not in ("
				+ orginal_SQL_predicates
				+ ") and  p.oid= pc.proj and pc.oid = pch.column_id and issuperprojection group by pc.name order by 2 desc limit 2 ";

		rs_pkeys = stmt.executeQuery(sql_chunks);
		handleWarnings(rs_pkeys.getWarnings());
		Map<String, String> treeMap = getChunkKeys(rs_pkeys);
		printMap(treeMap);
		primaryKey = getMapKey(treeMap);

		return primaryKey;
	}

	private static void handleWarnings(SQLWarning warn) {
		while (warn != null) {
			System.out.println("[" + warn.getErrorCode() + "] " + warn.getMessage());
			warn = warn.getNextWarning();
		}
	}

	public void findOSConfig() throws IOException {

		System.out.println("Available processors (cores): " + Runtime.getRuntime().availableProcessors());
		outputBuffer.write("Available processors (cores): " + Runtime.getRuntime().availableProcessors());
		outputBuffer.newLine();

		/* Total amount of free memory available to the JVM */
		System.out.println("Free memory (bytes): " + Runtime.getRuntime().freeMemory());
		outputBuffer.write("Free memory (bytes): " + Runtime.getRuntime().freeMemory());
		outputBuffer.newLine();
		/* This will return Long.MAX_VALUE if there is no preset limit */
		long maxMemory = Runtime.getRuntime().maxMemory();
		/* Maximum amount of memory the JVM will attempt to use */
		System.out.println("Maximum memory (bytes): " + (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));
		outputBuffer.write("Maximum memory (bytes): " + (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));
		outputBuffer.newLine();

		/* Total memory currently available to the JVM */
		System.out.println("Total memory available to JVM (bytes): " + Runtime.getRuntime().totalMemory());
		outputBuffer.write("Total memory available to JVM (bytes): " + Runtime.getRuntime().totalMemory());
		outputBuffer.newLine();
		/* Get a list of all filesystem roots on this system */
		File[] roots = File.listRoots();

		/* For each filesystem root, print some info */
		for (File root : roots) {
			System.out.println("File system root: " + root.getAbsolutePath());
			System.out.println("Total space (bytes): " + root.getTotalSpace());
			System.out.println("Free space (bytes): " + root.getFreeSpace());
			System.out.println("Usable space (bytes): " + root.getUsableSpace());
		}

	}

	public void fetchData(String original_sql, String tableName) throws SQLException, IOException {

		conn = DriverManager.getConnection(connection_str);
		stmt = conn.createStatement();

		int verticaDBVersion = conn.getMetaData().getDatabaseMajorVersion();

		if (verticaDBVersion == 6 || verticaDBVersion == 7) {
			System.out.println("Running with tested Vertica DB Version.. " + verticaDBVersion);
			outputBuffer.write("Running with tested Vertica DB Version.. " + verticaDBVersion);
			outputBuffer.newLine();

		} else {
			System.out.println("Warning: Running with untested Vertica DB Version.. " + verticaDBVersion);
			outputBuffer.write("Warning: Running with untested Vertica DB Version.. " + verticaDBVersion);
			outputBuffer.newLine();

		}

		if (this.load_bal.equalsIgnoreCase("1") && verticaDBVersion >= 7)
			this.connection_str = this.connection_str.concat("&ConnectionLoadBalance=1");
		System.out.println("Connection string... " + this.connection_str);
		outputBuffer.write("Connection string... " + this.connection_str);
		outputBuffer.newLine();

		System.out.println("Running on Node: " + runSimpleSQL("SELECT NODE_NAME FROM V_MONITOR.CURRENT_SESSION"));
		outputBuffer.write("Running on Node: " + runSimpleSQL("SELECT NODE_NAME FROM V_MONITOR.CURRENT_SESSION"));
		outputBuffer.newLine();

		String primaryKey = " ";
		rs_pkeys = conn.getMetaData().getPrimaryKeys(null, null, tableName);

		if (rs_pkeys != null) {
			handleWarnings(rs_pkeys.getWarnings());
			while (rs_pkeys.next()) {
				primaryKey = primaryKey + rs_pkeys.getString("COLUMN_NAME") + ",";
				// System.out.println("COLUMN_NAME = " + primaryKey);
			}
			primaryKey = primaryKey.substring(0, primaryKey.length() - 1);
			// System.out.println("pkeys " + primaryKey);
			// System.out.println("pkeys length" + primaryKey.length());

			// TODO: Need sql parser code to find existing predicates in
			// original sql. These should then be used as a filter since these
			// would not be good columns
			// to use for chunking
			//

			String orginal_SQL_predicates = "'col1,col2'";
			if (primaryKey.length() == 0) {
				String has_stats_sql = "select has_statistics from projections where anchor_table_name ='" + tableName
						+ "' and is_super_projection limit 1";
				String has_stats = runSimpleSQL(has_stats_sql);

				// String has_stats = find_stats(tableName);
				System.out.println("Has statistics: " + has_stats);
				outputBuffer.write("Has statistics: " + has_stats);
				outputBuffer.newLine();

				if (has_stats.equalsIgnoreCase("false")) {
					System.out.println("Need to run: " + this.analyze_stats);
					outputBuffer.write("Need to run: " + this.analyze_stats);
					outputBuffer.newLine();
					if (this.analyze_stats.equalsIgnoreCase("1")) {
						String sql_runstats = "select analyze_statistics('" + tableName + "')";
						runSimpleSQL(sql_runstats);
					}
				}

				primaryKey = findChunkCols(orginal_SQL_predicates, tableName);
			}
			/*
			 * TODO: if primarykey lenght is 0 then should find the table with
			 * the largest number of rows and use that table to find the column
			 * with the largest number distinct values using the following
			 * sql... select pc.name, sum(distinct_count) from vs_projections p
			 * , vs_projection_columns pc, vs_projection_column_histogram pch
			 * where anchortablename = 'part' and pc.name not in
			 * ('colinwhereclause1', 'colinwherecluase2') and p.oid= pc.proj and
			 * pc.oid = pch.column_id group by pc.name order by 2 desc limit 2 ;
			 * Pick 2 columns not in where clause and find the column with
			 * largest distinct values. For cases where the table did not
			 * originally have a primary key there might be use cases when using
			 * more than one column for the primary key introduces skew. It
			 * might be better to change this code to return a hashmap that then
			 * can compare to make sure there is no skew in the results. They
			 * query below would then have to be run again before the parallel
			 * code is run.
			 */

			String sql_chunks = "SELECT count(*) , hash(" + primaryKey + ") % " + noOfThreads + " FROM " + tableName
					+ " group by 2 ";

			rs = stmt.executeQuery(sql_chunks);

			printResults2(rs);

			String mod_sql = null;
			boolean keys = true;
			if (print_only.equalsIgnoreCase("0")) {
				for (int i = 0; i < noOfThreads && keys; i++) {

					try {
						/*
						 * TODO: Need to add code to find the right where clause
						 * and append the new modifications. The following
						 * example assumes simple sql with no subselects etc....
						 * 
						 * Note: limit is only done for testing purposes
						 */
						mod_sql = original_sql + " and hash(" + primaryKey + ") % " + noOfThreads + " = " + i
											+ " ;";
					//	mod_sql = original_sql + " and hash(" + primaryKey + ") % " + noOfThreads + " = " + i
					//			+ " limit 500 ;";
						System.out.println("Chunked SQL:" + mod_sql);
						outputBuffer.write("Chunked SQL:" + mod_sql);
						outputBuffer.newLine();

						RunnableFetch2 run = new RunnableFetch2(connection_str, "thread-" + (i + 1), mod_sql,
								primaryKey, startTime);
						run.start();
						start = end;
						if (primaryKey.length() == 0)
							keys = false;

					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			} else {
				System.out.println("Not Running query... debug print mode only.....");
			}

		} else {
			System.out.println("null value for ....");

		}

        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }

	}

}

class RunnableFetch2 implements Runnable {

	private Thread t;
	private String threadName;
	Connection conn = null;
	Statement stmt = null;
	int startData = 0;
	int endData = 0;
	ResultSet rs = null;
	String tableName = null;
	String newsql = null;
	String columnName = null;
	long startTime = 0;

	RunnableFetch2(String connection_str, String name, String newsql, String primaryKey, long startTime)
			throws SQLException {
		threadName = name;

		this.newsql = newsql;

		this.startTime = startTime;
		columnName = primaryKey;

		conn = DriverManager.getConnection(connection_str);
		stmt = conn.createStatement();

	}

	public void start() {
		// System.out.println("Starting " + threadName );
		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}

	@Override
	public void run() {
		try {

			rs = stmt.executeQuery(newsql);

			printResults(rs, threadName);

			long endTime = System.currentTimeMillis();
			System.out.println("thread " + threadName + " end at: " + endTime + "\n\n");
			System.out.println("total time taken: " + (endTime - startTime) + "\n\n");
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
			

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	protected void printResults(ResultSet rs, String threadNm) throws IOException {

		System.out.println("ThreadName " + threadNm);
 		BufferedWriter outputBufferThread = null;
		int SIZEOFBUFFER = 1500;


		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int nCols = rsmd.getColumnCount();
			String outputfilename = threadNm + "cols" + nCols + ".txt";
			
			FileWriter outputFW = new FileWriter(outputfilename);
			outputBufferThread = new BufferedWriter(outputFW, SIZEOFBUFFER);
			
			StringBuffer sb = new StringBuffer();

			for (int i = 1; i <= nCols; i++) {
				if (i != 1)
					sb.append(", ");
				sb.append(rsmd.getColumnLabel(i));
			}

			// print column headers to stdout
			System.out.println("Headers for the output are: " + sb.toString());
			 

			// print results to stdout
			while (rs.next()) {
				// use clear() or trim() when they are available for
				// StringBuffers
				StringBuffer sb1 = new StringBuffer();
				for (int i = 1; i <= nCols; i++) {
					if (i != 1)
						sb1.append(", ");
					sb1.append(rs.getObject(i));
				}

				//System.out.println(sb1.toString());
				outputBufferThread.write(sb1.toString());
				outputBufferThread.newLine();
			}

		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			if (outputBufferThread != null)
				outputBufferThread.close();
		}

	}

}
