#!/usr/bin/python

from bdb_capacity import BdbJeCapacityModel
import commands
import math
import os
import shutil
import sys
from optparse import OptionParser

MAX_DATA_SIZE_GB=512
MAX_CACHE_SIZE_MB=20480

storeSizes=[100000000,200000000]
keySizes=[12,64,128]
valueSizes=[100,1024,10240]
requestRates=[250,500,1000]


def overrideParamInPropertyFile(runName, propFile, params):
    propFile = open(propFile, 'r')
    runPropFileName = runName+".properties"
    runPropFile = open(runPropFileName, "w")
    for propertyLine in propFile :
        if propertyLine == "":
            continue
        overridden=False
        for param in params :
            paramName = param[0]
            paramVal  = param[1]
            if paramName in propertyLine :
                runPropFile.write(paramName+"="+paramVal+"\n")
                overridden = True
                break
        if not overridden :
            runPropFile.write(propertyLine)
    propFile.close()
    runPropFile.close()
    return runPropFileName


if __name__ == "__main__":
    usage = "usage: %prog "
    parser = OptionParser(usage)
    parser.add_option("-d", "--max-disk-space", dest="maxDiskSpace",
                      default = MAX_DATA_SIZE_GB, help="maximum amount of disk space to use for runs")
    parser.add_option("-m", "--max-memory", dest="maxMemory",
                      default = MAX_CACHE_SIZE_MB, help="maximum amount of memory to use for runs")
    parser.add_option("-t", "--workload-run-time", dest="workloadRunTime",
                      default = 1800, help="amount of time in secs for each workload")
    parser.add_option("-c","--data-copy-location", dest="dataCopyLocation",
                      default = "~/" ,help="location to copy data across runs, to save data generation time")
    parser.add_option("-p","--properties", dest="storePropsFile",
                      default = "store.properties" ,help="path to store's properties file")
    parser.add_option("-r","--dry-run", dest="dryRun", action="store_true",
                      default = False ,help="If specified just print what workloads will be run..")
    parser.add_option("-j","--jvm-args", dest="jvmargs",
                      default = "-server -Xms32g -Xmx32g -XX:NewSize=2048m -XX:MaxNewSize=2048m -XX:+AlwaysPreTouch -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -XX:SurvivorRatio=2 -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -Xloggc:loggc -XX:+PrintGCTimeStamps" ,help="JVM args to use to run perf tool")
    
   
    # TODO make overriding store configs based on workload an option and 'pluggable', so we can quickly iterate
    # on any perf runs by varying different input parameters.
    (options, args) = parser.parse_args()

    for storeSize in storeSizes :
        for keySize in keySizes :
            for valueSize in valueSizes :
                dataSetName = str(storeSize)+"_"+str(keySize)+"_"+str(valueSize)
                
                # check if we will have enough disk space 
                model = BdbJeCapacityModel(storeSize,
                                               keySize,
                                               valueSize,
                                               1,
                                               1,
                                               '~/tools/je-4.1.17.jar')
                model.compute()
                if  model.getDataSizeInGB() > options.maxDiskSpace :
                    continue    
                
                # generate data once, warm it up.. copy it and keep using
                print 'Generating data for :', storeSize, keySize, valueSize
                perfRunCmd = ["java " + options.jvmargs ,  
                                " -cp .:bin:lib/je.jar:lib/jopt-simple-4.3.jar:lib/krati-0.4.7-SNAPSHOT.jar:lib/log4j-1.2.17.jar ",
                                "-Dlog4j.configuration=file://`pwd`/log4j.properties PerformanceTest ",
                                "--store-type bdb ",
                                "--threads 50 ",
                                "--key-size " +  str(keySize) + " " ,
                                "--value-size "+ str(valueSize) + " ",
                                "--warm-up --prepopulate "+ " "
                                "--directory bdbdata ",
                                "--reporting-interval 60  ",
                                "--percent-gets 75 ",
                                "--percent-puts 25 ",
                                "--percent-deletes 0 ",
                                "--runtime 1 ",
                                "--throughput " + str(1) + " " ,
                                "--num-keys "+ str(storeSize) + " ",
                                "--store-properties "+ options.storePropsFile + " ",
                                "--results-directory `pwd`",
                                ">> genout"];
                perfRunCmd = " ".join(perfRunCmd)
                print perfRunCmd
                if not options.dryRun :
                    os.system(perfRunCmd);
                    os.system("mv bdbdata "+options.dataCopyLocation+"/"+dataSetName)
            
                print 'Copying data...' 
                for requestRate in requestRates :
                    getRate = 3 * requestRate
                    putRate = requestRate
                    runName = str(storeSize)+"_"+str(keySize)+"_"+str(valueSize)+"_"+str(getRate)+"_"+str(putRate)
                    
                    model = BdbJeCapacityModel(storeSize,
                                   keySize,
                                   valueSize,
                                   getRate,
                                   putRate,
                                   '~/tools/je-4.1.17.jar')
                    model.compute()
                    
                    print runName, model.getDataSizeInGB(), model.getMinCacheinMB()
                    # check if the resulting run will have enough memory 
                    if model.getMinCacheinMB() >= int(options.maxMemory) :
                        continue
                                        
                    propFileName = overrideParamInPropertyFile(runName, options.storePropsFile, [["je.cache.size", str(int(model.getMinCacheinMB()) * 1024 * 1024)]])
                    print "Running workload :", "storeSize", storeSize, "keySize", keySize, "valueSize", valueSize, "getRate", getRate, "putRate" , putRate, "MINCACHE",model.getMinCacheinMB()
                    
                    if not options.dryRun :
                        os.system("mkdir -p bdbdata")
                        os.system("cp -rf  "+ options.dataCopyLocation+"/"+dataSetName+"/* bdbdata/");
                    perfRunCmd = ["java " + options.jvmargs ,  
                                " -cp .:bin:lib/je.jar:lib/jopt-simple-4.3.jar:lib/krati-0.4.7-SNAPSHOT.jar:lib/log4j-1.2.17.jar ",
                                "-Dlog4j.configuration=file://`pwd`/log4j.properties PerformanceTest ",
                                "--store-type bdb ",
                                "--threads 50 ",
                                "--key-size " +  str(keySize) + " " ,
                                "--value-size "+ str(valueSize) + " ",
                                "--directory bdbdata ",
                                "--reporting-interval 60  ",
                                "--percent-reads 75 ",
                                "--percent-writes 25 ",
                                "--percent-deletes 0 ",
                                "--runtime  "+ str (options.workloadRunTime) + " ",
                                "--throughput " + str(getRate + putRate) + " " ,
                                "--num-keys "+ str(storeSize) + " ",
                                "--store-properties "+ propFileName + " ",
                                "> out"];
                    perfRunCmd = " ".join(perfRunCmd)
                    print perfRunCmd
                    
                    if not options.dryRun :
                        os.system(perfRunCmd);
                        os.system("mkdir -p "+ runName)
                        os.system("mv -f loggc "+ runName +"/")
                        os.system("mv -f bdb.stats "+ runName +"/")
                        os.system("mv -f latency.out "+ runName +"/")
                        os.system("mv -f out "+ runName +"/")
                        os.system("mv -f "+ propFileName +" " + runName +"/")
                        os.system("rm -rf bdbdata")
      
                if not options.dryRun :
                    # should delete the generated data
                    os.system("rm -rf "+options.dataCopyLocation+"/"+dataSetName)

