package kvperf;
import java.util.Iterator;
import java.util.Properties;


public interface KVStore {
	
	public void init(Properties props) throws Exception;
	
	
	public void beginPrepopulate() throws Exception ;
	
	public void endPrepopulate()throws Exception ;
	
	
	public void beginWarmup();
	
	public void endWarmup();
	
	public boolean isWarmedup();
	
	
	public void put(byte[] key, byte[] value) throws Exception;
	
	public byte[] get(byte[] key) throws Exception;

	public void delete(byte[] key) throws Exception;
	
	public Iterator<byte[]> values() throws Exception;
	
	public void reportDetailedStats() throws Exception;
	
	public void close() throws Exception;	
}
