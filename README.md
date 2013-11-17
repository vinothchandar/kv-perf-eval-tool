kv-perf
=======

Performance testing framework for key-value datastores

Runs key-value workloads against different databases and measures performance.

Currently supports the following

- Oracle Berkeley DB Java Edition
- MySQL
- Krati
- LevelDB (JNI)

Requirements:
Python 2.6+
Java 6+

Building :
$ ant 

Usage :
$ java -cp dist/kv-perf-tool.jar kvperf.PerformanceTest --help
Option                                  Description                            
------                                  -----------                            
--cleanup                               Delete the contents of the data        
                                          directory when completed.            
--directory <data_directory>            The directory in which to write data.  
--help                                  Print this help message.               
--key-size <Integer: key_size_bytes>    The number of bytes in the key.        
                                          (default: 8)                         
--num-keys <Integer: num_keys>          The number of unique keys to use if    
                                          not reading keys from a file.        
                                          (default: 10000000)                  
--ops <Long: ops>                       The total number of operations to do.  
                                          (default: 0)                         
--percent-deletes <Double:              The percentage of requests that are    
  percent_deletes>                        deletes [0-100]. (default: 5.0)      
--percent-gets <Double: percent_gets>   The percentage of requests that are    
                                          gets [0-100]. (default: 60.0)        
--percent-puts <Double: percent_puts>   The percentage of requests that are    
                                          puts [0-100]. (default: 35.0)        
--prepopulate                           Prepopulate with sequential keys.      
                                          Requires the num-keys option to be   
                                          present. Unlike the warm-up option   
                                          this will not generate any garbage,  
                                          but is very fast.                    
--reporting-interval <Integer:          The number of seconds in between perf  
  reporting_interval_secs>                stat reports. (default: 2147483647)  
--results-directory <results_directory> The directory in which to write        
                                          results to. (default: result)        
--runtime <Long: runtime_secs>          Total number of seconds to run         
                                          workload. (default: 600)             
--scan-thread-sleep <Integer:           The time to sleep (in ms) between      
  scan_thread_sleep_ms>                   scans. (default: 0)                  
--scan-threads <Integer: scan_threads>  The number threads doing background    
                                          scans on the store. (default: 0)     
--store-properties <store-properties>   Pick up the config properties for the  
                                          store from this file                 
--store-type <type>                     Store type {bdb, mysql, krati etc}     
--threads <Integer: num_threads>        The number of concurrent threads       
                                          making requests. (default: 1)        
--throughput <Integer: req_per_sec>     The target throughput (req/sec) to     
                                          maintain. (default: -1)              
--value-size <Integer:                  The number of bytes in the values.     
  value_size_bytes>                       (default: 100)                       
--warm-up                               warmp up the store before experiments  
                                          begin 

Sample run (BDB) :
$ java -server -Xms1g -Xmx1g -XX:NewSize=128m -XX:MaxNewSize=128m -XX:+AlwaysPreTouch -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -XX:SurvivorRatio=2 -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -Xloggc:results/gc.log -XX:+PrintGCTimeStamps -cp dist/kv-perf-tool.jar -Dlog4j.configuration=file://`pwd`/log4j.properties kvperf.PerformanceTest --store-type bdb --threads 5 --key-size 10  --min-value-size 100  --max-value-size 200  --warm-up --prepopulate  --directory bdbdata --reporting-interval 10   --percent-gets 100  --percent-puts 0  --percent-deletes 0  --runtime 60 --throughput 100 --num-keys 1000000 --store-properties sample_config/store.properties --results-directory results

MemoryHogger :

Also included is a Memory hogger tool to lock down memory so we can hit the disk as hard as we want.

$ sudo swapoff -a
$ java -Xmx12g -Xms12g -XX:+AlwaysPreTouch -cp dist/kv-perf-tool.jar MemoryHogger


