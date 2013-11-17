package kvperf;
import java.util.ArrayList;
import java.util.List;

/**
 * Run this like this with swap turned off
 * 
 * sudo swapoff -a
 * java -Xmx12g -Xms12g -XX:+AlwaysPreTouch -cp dist/kv-perf-tool.jar MemoryHogger 
 * 
 * @author vchandar
 *
 */
public class MemoryHogger {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Runtime runtime = Runtime.getRuntime();

		long avail = runtime.freeMemory();
		int blockSize = 1024 * 1024;
		long allocs = (long) (0.7 * (avail/blockSize));

		List<byte[]> hoggerList = new ArrayList<byte[]>();
		for (int alloc=0; alloc < allocs; alloc++){
			hoggerList.add(new byte[blockSize]);
		}
		System.out.println("Okay, allocated as much as I can.. dozing off..");
		
		Thread.sleep(Integer.MAX_VALUE);
		
	}
}
