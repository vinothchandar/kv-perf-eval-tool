package kvperf;
import java.util.Iterator;
import java.util.Properties;

import krati.store.DataStore;


public class KratiStore implements KVStore {
	
	private final DataStore<byte[], byte[]> store;

	public KratiStore(DataStore<byte[],byte[]> store) {
		super();
		this.store = store;
	}

	@Override
	public void put(byte[] key, byte[] value) throws Exception {
		this.store.put(key, value);
	}

	@Override
	public byte[] get(byte[] key) {
		return store.get(key);
	}

	@Override
	public void delete(byte[] key) throws Exception {
		store.delete(key);
	}

	@Override
	public Iterator<byte[]> values() {
		throw new UnsupportedOperationException();
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
		store.close();
	}

	@Override
	public void init(Properties props) {
		// TODO Auto-generated method stub
		
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
