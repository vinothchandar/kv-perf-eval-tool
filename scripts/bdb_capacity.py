#!/usr/bin/python
"""
Computes the capacity needed on a voldemort host, to house a given store
"""
from optparse import OptionParser
import commands
import os
import shutil
import sys
import math

BLOCKSIZE=4092.0
GB=1073741824.0
MB=1048576.0
KB=1024.0
UTILIZATION=0.50
NUMCLEANERS=1
LOGFILESIZE=60 * MB
CLEANER_BYTES_INTERVAL= 30 * MB
CHECKPOINTER_BYTES_INTERVAL= 2 * GB
ADMIN_MEMORY_USAGE_PCT=0.1

# we will shoot for some cleaner frequency.
CLEANER_WAKEUP_INTERVAL_SECS=30

# account for extra clocks etc added to voldemort value (1 server clock) 
#(Assuming higher #server-ids in clock is not the worst case in terms of memory needed for cleaning)
VALUE_PADDING=18

# we do one BDB allocation, one BDB checksum buffer, one temporary buffer in voldemort + 1 for all the temporary allocs
# This is not science and can't be.
ONLINE_BUFFER_FUDGE=4

# this is basically the BTree fanout (in reality this is lower than 512 since some BINs are not full)
LNS_PER_BIN=512

"""
Takes as input
1. number of keys
2. average key size
3. average value size
4. gets/sec
5. puts/sec
"""
class BdbJeCapacityModel :
    def __init__(self, nKeys, keySize, valueSize, getRate, putRate, jarFile):
        self.nKeys = nKeys
        self.keySize = keySize
        self.valueSize = valueSize + VALUE_PADDING
        self.getRate = getRate
        self.putRate = putRate
        self.jarFile = jarFile

        # stuff that I will compute later
        self.indexNodeSize = 0
        self.dataNodeSize = 0
        self.depthOfTree = 0
        self.totalIndexCacheSize = 0
        self.totalCacheSize = 0
        self.totalUpperINs = 0
        self.totalBINs=0
        
        # different memory areas in BDB cache
        self.upperINAreaSize=0
        self.cleanerOperationalAreaSize=0
        self.onlineAreaSize=0
        self.BINStagingAreaSize=0
        self.allocRate=0

        self.mincache = 0;
        self.avglatency = []
        self.jeCmd = 'java -cp '+ self.jarFile + ' com.sleepycat.je.util.DbCacheSize -records '+ str(self.nKeys) + ' -key '+ str(self.keySize) + ' -data '+ str(self.valueSize)+' -nodemax '+ str(LNS_PER_BIN) 

    def compute(self):
        jeOutput = commands.getstatusoutput(self.jeCmd)[1].split("\n")
        #print jeOutput
        linecnt=0
        for line in jeOutput :
            if line == "" or line == None or "===" in line or "Inputs:" in line or "------" in line:
                continue
            # process all the cache sizes
            if linecnt >= 1 and linecnt <= 4 :
                # skip the minimum sizes for now
                if linecnt == 2 :
                    self.totalIndexCacheSize = cleanNumber(line.split()[0])
                elif linecnt == 4 :
                    self.totalCacheSize = cleanNumber(line.split()[0].strip())
                pass
            elif linecnt > 5 :
                self.depthOfTree = self.depthOfTree + 1
                linesplit = line.split()
                self.indexNodeSize = cleanNumber(linesplit[1])/cleanNumber(linesplit[2])
                if linecnt > 6 :
                    self.totalUpperINs = self.totalUpperINs + cleanNumber(linesplit[2])
                elif linecnt == 6 :
                    #TODO in practice, I see more total BINs than what DbCache says due to opportunistic splitting
                    self.totalBINs = cleanNumber(linesplit[2])

            linecnt = linecnt + 1
        
        self.dataNodeSize = (self.totalCacheSize - self.totalIndexCacheSize)/ self.nKeys

        # Compute space needed for the Upper INs. This is pretty static
        self.upperINAreaSize = (self.indexNodeSize * self.totalUpperINs)
        
        # Compute space needed for the online traffic
        # TODO the index space is sort of double counted below in BINFetchesTotal as well.. So can remove this 
        self.onlineAreaSize = ONLINE_BUFFER_FUDGE * ((self.getRate + self.putRate) * (self.indexNodeSize + self.dataNodeSize))
        
        # Compute the extra memory needed to keep the data read in by the cleaner
        # Note 1 : if you assume the cleaner will always fetch all LNs (worst case, since it will  bring in more BINs), you will need 
        # way more memory just for the index. 
        # Note 2 : You cannot assume there will be uniform distribution of BINs and LNs in the logs. This is not realistic since
        # more than one LN attaches to a BIN.. ideal LN:BIN ratio is FANOUT:1
        # TODO since we cannot guarantee that a log file cleaned will be half full.. Should we consider the possibility of everything being live as worst case
        estimatedLiveLNsPerLogFile =  (UTILIZATION * LOGFILESIZE) / (self.dataNodeSize + (self.indexNodeSize/LNS_PER_BIN))
        estimatedDeadLNsPerLogFile = ((1.0-UTILIZATION) * LOGFILESIZE) / (self.dataNodeSize + (self.indexNodeSize/LNS_PER_BIN))
        
        
        # Compute space needed for cleaner to read in files and operate on them
        self.cleanerOperationalAreaSize = NUMCLEANERS * LOGFILESIZE
        # All these LNs will take space in the cache. 
        self.cleanerOperationalAreaSize += (self.dataNodeSize * (estimatedLiveLNsPerLogFile + estimatedDeadLNsPerLogFile))
        
        # Compute how much extra memory is needed to stage enough BINs such that we meet some cleaner wakeup interval
        filesReadPerCleanerRun = NUMCLEANERS * math.ceil(CLEANER_BYTES_INTERVAL/LOGFILESIZE)
        
        # the second term below basically how much live LNs can be present in a log and account for memory to bring in their BINs 
        BINfetchesDirty = (CLEANER_WAKEUP_INTERVAL_SECS * self.putRate) + (filesReadPerCleanerRun *  estimatedLiveLNsPerLogFile)
        
        # TODO There is no guarantee a non dirty BIN will be evicted in preference to a dirty BIN
        # this captures the probability that something we evict out is 'dirty', in case eviction is modelled better later
        BINfetchesNonDirty = (CLEANER_WAKEUP_INTERVAL_SECS * self.getRate)
        BINfetchesTotal = BINfetchesDirty + BINfetchesNonDirty
        dirtyProbability = BINfetchesDirty / BINfetchesTotal
        
        # this much will be written by the cleaner for sure
        cleanerWriteBytes = filesReadPerCleanerRun * estimatedLiveLNsPerLogFile * self.dataNodeSize
        onlineWriteBytes = CLEANER_WAKEUP_INTERVAL_SECS * self.putRate * self.dataNodeSize
        
        # we will write everything dirty in memory. assume upperINs will be always dirty
        checkpointerWriteBytes = (CLEANER_BYTES_INTERVAL/CHECKPOINTER_BYTES_INTERVAL) * (self.upperINAreaSize + (self.indexNodeSize * BINfetchesDirty))
        # Now, we will adjust the amount of BINs written out so that, we don't invoke cleaner as often
        # TODO for now, budget for non dirty BINs also
        BINsToBeStaged = BINfetchesTotal + ((cleanerWriteBytes + onlineWriteBytes + checkpointerWriteBytes - CLEANER_BYTES_INTERVAL) / (self.indexNodeSize))
        #BINsToBeStaged = BINfetchesDirty + ((cleanerWriteBytes + onlineWriteBytes + checkpointerWriteBytes - CLEANER_BYTES_INTERVAL) / (self.indexNodeSize))
        self.BINStagingAreaSize = BINsToBeStaged * self.indexNodeSize
        if self.BINStagingAreaSize > (self.totalBINs * self.indexNodeSize) :
            self.BINStagingAreaSize = self.totalBINs * self.indexNodeSize
        
        
        self.allocRate = self.onlineAreaSize + (self.cleanerOperationalAreaSize/CLEANER_WAKEUP_INTERVAL_SECS) 
        self.mincache = self.upperINAreaSize + self.onlineAreaSize + self.cleanerOperationalAreaSize + self.BINStagingAreaSize
        
        # There is some administrative memory overhead (the utilization summary db, log write buffers etc) 
        self.mincache = self.mincache + (self.mincache * ADMIN_MEMORY_USAGE_PCT) 
        
        # TODO compute average latency
        pass

        # TODO compute IOPS
        pass
    
    def getDataSizeInGB(self):
        return (self.totalCacheSize/UTILIZATION)/ GB
    
    def getMinCacheinMB(self):
        return self.mincache/MB

    def display(self):
        print self.jeCmd
        print '******************* Capacity Analysis *********************'
        print 'Total Index size :', formatNumber(self.totalIndexCacheSize)
        print 'Total Data  size :', formatNumber(self.totalCacheSize), ' at ', UTILIZATION, ' utilization :', formatNumber(self.totalCacheSize/UTILIZATION)
        print 'Tree depth       :', self.depthOfTree
        print 'Total Upper INs  :', self.totalUpperINs
        print 'Total BINs       :', self.totalBINs
        print 'Leaf Node size   :', formatNumber(self.dataNodeSize)
        print 'Index Node size  :', formatNumber(self.indexNodeSize)
        print '\n'
        print 'UpperIN Area     :', formatNumber(self.upperINAreaSize)
        print 'Online Area      :', formatNumber(self.onlineAreaSize)
        print 'BIN staging Area :', formatNumber(self.BINStagingAreaSize)
        print 'Cleaning Area    :', formatNumber(self.cleanerOperationalAreaSize)
        print 'MINCACHE         :', formatNumber(self.mincache), ' [', int(self.mincache/MB),'MB]'
        #print 'allocBytes/sec   :', formatNumber(self.allocRate)

def cleanNumber(num):
    return int(num.replace(",","").strip())

def formatNumber(num):
    if num < KB :
        return str(round(num,1)) +' bytes'
    elif num < MB :
        return str(round(num / KB, 1)) +' KB'
    elif num < GB :
        return str(round(num/MB,1)) + ' MB'
    else :
        return str(round(num/GB,1)) + ' GB'


if __name__ == "__main__":
    usage = "usage: %prog "
    parser = OptionParser(usage)
    parser.add_option("-n", "--nkeys", dest="nkeys",
                      help="number of keys")
    parser.add_option("-k", "--keysize", dest="keysize",
                      help="average size (bytes) of each key")
    parser.add_option("-v","--valuesize", dest="valuesize",
                      help="average size (bytes) of each value")
    parser.add_option("-g","--getrate", dest="getrate",
                      help="gets per second, including getalls")
    parser.add_option("-p","--putrate", dest="putrate",
                      help="puts per second")
    parser.add_option("--jar", default="~/tools/je-4.1.17.jar", dest="jarfile",
                      help="Path to bdb-je JAR file")
    (options, args) = parser.parse_args()

    model = BdbJeCapacityModel(int(options.nkeys),
                                   int(options.keysize),
                                   int(options.valuesize),
                                   int(options.getrate),
                                   int(options.putrate),
                                   str(options.jarfile))
    model.compute()
    model.display()
