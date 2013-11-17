package kvperf;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Logger;


public class MysqlStore implements KVStore {
	private static final Logger logger = Logger.getLogger(MysqlStore.class);
	private Connection conn = null;
	private String storename;
	private String mysqlUsername = "";
	private String mysqlPassword = "";
	private String mysqlDbName   = "testdb";
	final private int mysqlPort = 8889;
	private File resultsDirectory;

	@Override
	public void init(Properties props) throws Exception {
		// TODO Auto-generated method stub
		try {
			conn = DriverManager.getConnection("jdbc:mysql://localhost:"+mysqlPort+"/"+mysqlDbName+"?" +
					"user="+mysqlUsername+"&password="+mysqlPassword);
			execute("create table if not exists " + storename
					+ " (key_ varbinary(200) not null, "
					+ " value_ blob, primary key(key_)) engine = InnoDB");
		} catch (SQLException ex) {
			// handle any errors
			System.err.println("SQLException: " + ex.getMessage());
			System.err.println("SQLState: " + ex.getSQLState());
			System.err.println("VendorError: " + ex.getErrorCode());
			ex.printStackTrace();
		}
	}
	
	public MysqlStore(String storename, File resultsDirectory) throws Exception {
		this.storename = storename; 
		this.resultsDirectory = resultsDirectory;
	}

	@Override
	public void put(byte[] key, byte[] value) throws Exception {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String insert = "insert into " + storename + " (key_, value_) values (?, ?) ON DUPLICATE KEY UPDATE value_ = ?";
		try {
			/* update or insert */
			stmt = conn.prepareStatement(insert);
			stmt.setBytes(1, key);
			stmt.setBytes(2, value);
			stmt.setBytes(3, value);
			stmt.executeUpdate();
		}
		catch (SQLException ex){
			// handle any errors
			System.err.println("SQL :"+ insert);
			System.err.println("SQLException: " + ex.getMessage());
			System.err.println("SQLState: " + ex.getSQLState());
			System.err.println("VendorError: " + ex.getErrorCode());
			ex.printStackTrace();
		}
		finally {
			tryClose(rs);
			tryClose(stmt);
		}
	}	

	@Override
	public byte[] get(byte[] key) throws Exception {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String select = "select value_ from " + storename + " where key_ = ?";
		try {
			stmt = conn.prepareStatement(select);
			stmt.setBytes(1, key);
			rs = stmt.executeQuery();
			if(rs.next()) {
				byte[] value = rs.getBytes("value_");
				return value;
			} else {
				return null;
			}
		}
		catch (SQLException ex){
			// handle any errors
			System.err.println("SQL :"+ select);
			System.err.println("SQLException: " + ex.getMessage());
			System.err.println("SQLState: " + ex.getSQLState());
			System.err.println("VendorError: " + ex.getErrorCode());
			ex.printStackTrace();
		}
		finally {
			tryClose(rs);
			tryClose(stmt);
		}
		return null;
	}

	@Override
	public void delete(byte[] key) throws Exception {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String delete = "delete from " + storename + " where key_ = ?";

		try {
			stmt = conn.prepareStatement(delete);
			stmt.setBytes(1, key);
			stmt.executeUpdate();
			return ;
		} catch(SQLException e) {
			throw new Exception("Fix me!", e);
		} finally {
			tryClose(rs);
			tryClose(stmt);
		}
	}

	// be careful when using this. If you does not iterator to the end, make sure you call remove() to close statements
	@Override
	public Iterator<byte[]> values() throws Exception {
		String select = "select value_ from " + storename;
		try {
			final PreparedStatement stmt = conn.prepareStatement(select);
			final ResultSet rs = stmt.executeQuery();
			return new Iterator<byte[]>(){
				public byte[] next() {
					try {
						byte[] value = rs.getBytes("value_");
						return value;
					} catch (SQLException e) {
						logger.error("Error when getting result from resultSet" + e);
						return null;
					}
				}

				public boolean hasNext() {
					try {
						if(rs.next()) {
							return true;
						} else {
							tryClose(rs);
							tryClose(stmt);
							return false;
						}
					} catch (SQLException e) {
						logger.error("Error when getting result from resultSet" + e);
						return false;
					}
				}

				public void remove() {
					tryClose(rs);
					tryClose(stmt);
				}
			};
		}
		catch (SQLException ex){
			// handle any errors
			System.err.println("SQLException: " + ex.getMessage());
			System.err.println("SQLState: " + ex.getSQLState());
			System.err.println("VendorError: " + ex.getErrorCode());
			throw new Exception(ex);
		}
	}

	public void truncate() throws Exception {
		execute("DROP TABLE IF EXISTS " + storename);
	}
	
	public void reportDetailedStats() throws Exception{
		
	}
	
	public void beginWarmup(){
		return;
	}
	
	public void endWarmup(){
		return;
	}
	
	public boolean isWarmedup(){
		return true;
	}

	@Override
	public void close() throws Exception {
		tryClose(conn);
	}

	private void tryClose(ResultSet rs) {
		try {
			if(rs != null)
				rs.close();
		} catch(Exception e) {
			logger.error("Failed to close resultset." + e);
		}
	}

	private void tryClose(Connection c) {
		try {
			if(c != null)
				c.close();
		} catch(Exception e) {
			logger.error("Failed to close connection." + e);
		}
	}

	private void tryClose(Statement s) {
		try {
			if(s != null)
				s.close();
		} catch(Exception e) {
			logger.error("Failed to close prepared statement." + e);
		}
	}

	public boolean tableExists(String name) throws Exception {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String select = "show tables like '" + name + "'";
		try {
			stmt = conn.prepareStatement(select);
			rs = stmt.executeQuery();
			return rs.next();
		} catch(SQLException e) {
			throw new Exception("SQLException while checking for table existence!",e);
		} finally {
			tryClose(rs);
			tryClose(stmt);
		}
	}

	public void execute(String query) throws Exception {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(query);
			stmt.executeUpdate();
		} catch(SQLException e) {
			throw new Exception("SQLException while performing operation.", e);
		} finally {
			tryClose(stmt);
		}
	}

	@Override
	public void beginPrepopulate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endPrepopulate() {
		// TODO Auto-generated method stub
		
	}
}
