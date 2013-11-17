package kvperf;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;

import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteOptions;






public class LevelDBStore implements KVStore {

	
	private DB db;
	private File dataDir;
	private File resultsDirectory;

	
	public LevelDBStore(File envDir, File resultsDirectory){
		this.resultsDirectory = resultsDirectory;
		this.dataDir = envDir;
	}
	
	
	
	@Override
	public void init(Properties props) throws Exception {
		Options options = new Options();
		options.createIfMissing(true);
		options.paranoidChecks(true);
		options.compressionType(CompressionType.NONE);
		options.verifyChecksums(true);
		options.cacheSize(20*1024L*1024L*1024L);
		
		db = factory.open(dataDir, options);
	}

	@Override
	public void beginPrepopulate() throws Exception {
		// Nothing to do here
	}

	@Override
	public void endPrepopulate() throws Exception {
		// Nothing to do here
	}

	@Override
	public void beginWarmup() {
		// Nothing to do here ..
	}

	@Override
	public void endWarmup() {
		// Nothing to do here ..
	}

	@Override
	public boolean isWarmedup() {
		// let the warmup end on it own
		return false;
	}

	@Override
	public void put(byte[] key, byte[] value) throws Exception {
		WriteOptions wOpts = new WriteOptions();
		wOpts.sync(false);
		db.put(key, value);
	}

	@Override
	public byte[] get(byte[] key) throws Exception {
		ReadOptions rOpts = new ReadOptions();
		rOpts.verifyChecksums(true);
		rOpts.fillCache(true);
		return db.get(key);
	}

	@Override
	public void delete(byte[] key) throws Exception {
		WriteOptions wOpts = new WriteOptions();
		wOpts.sync(false);
		db.delete(key, wOpts);
	}

	@Override
	public Iterator<byte[]> values() throws Exception {
		return null;
	}

	@Override
	public void reportDetailedStats() throws Exception {
		
	}

	@Override
	public void close() throws Exception {
		if (db != null){
		  db.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.createIfMissing(true);
		options.paranoidChecks(true);
		options.compressionType(CompressionType.NONE);
		options.verifyChecksums(true);
		options.cacheSize(20*1024L*1024L*1024L);
		
		
		ReadOptions rOpts = new ReadOptions();
		rOpts.verifyChecksums(true);
		rOpts.fillCache(true);
		
		DB db = factory.open(new File("leveldb-example"), options);
		try {
			WriteOptions wOpts = new WriteOptions();
			wOpts.sync(false);
			
			db.put("test".getBytes(), "test-val".getBytes());
			System.out.println(new String(db.get("test".getBytes())));
		} finally {
		  // Make sure you close the db to shutdown the 
		  // database and avoid resource leaks.
		  db.close();
		}

	}

	
}
