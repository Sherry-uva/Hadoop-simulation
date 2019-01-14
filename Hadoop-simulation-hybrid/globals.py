from math import floor

# Global constants
NUM_RACK = 12
SET_RACK = [index for index in range(NUM_RACK)]
NUM_N0 = 3 # number of racks that have optical circuits
SET_RACK_N0 = [index for index in range(NUM_N0)]
NUM_HOST_PER_RACK = 20
SET_HOST = [index for index in range(NUM_RACK*NUM_HOST_PER_RACK)]
NUM_CNTR_PER_HOST = 16 # number of containers per host
SET_CNTR = [index for index in range(NUM_RACK*NUM_HOST_PER_RACK*NUM_CNTR_PER_HOST)]
RATE_INTRA_RACK = 8.0*1000**3
RATE_INTER_RACK = 10.0*1000**3
RATE_OCS = 100.0*1000**3 # bps
hostPerRack = dict()
for rack in SET_RACK:
	hostPerRack[rack] = [index for index in range(rack*NUM_HOST_PER_RACK, (rack+1)*NUM_HOST_PER_RACK)]
MAPPING_HOST_TO_RACK = dict()
for host in SET_HOST:
	MAPPING_HOST_TO_RACK[host] = int(floor(host/NUM_HOST_PER_RACK))
MAPPING_CNTR_TO_HOST = dict()
for container in SET_CNTR:
	MAPPING_CNTR_TO_HOST[container] = int(floor(container/NUM_CNTR_PER_HOST))
MAPPING_CNTR_TO_RACK = dict()
for container in SET_CNTR:
	MAPPING_CNTR_TO_RACK[container] = int(floor(container/(NUM_HOST_PER_RACK*NUM_CNTR_PER_HOST)))

# number of links between the OCS and the core EPS
NUM_LINK_OCS_EPS = 2
# number of links between the core EPS and the storage unit
# NUM_LINK_STORAGE = 5
# the length of each time slot used in advance reservation, in second
AR_SLOT = 0.01 
# advance reservation horizon, unit: AR_SLOT
AR_WINDOW = 5*10**4
# time interval to update flow rate on electrical paths
EPATH_UPDATE_INTERVAL = 0.001

RATIO_MAP_TO_REDUCE_NUMBER = 8
SIMLT_TIME = 1000
# if the shuffle data generated on one rack is larger than this threshold, then use the optical circuits
PER_RACK_SHUFFLE_THRESHOLD = 1.2*1024**3

# HDFS
HDFS_BLOCK_SIZE= 128*1024*1024*8 # in bits, HDFS block size
DISK_SIZE_PER_HOST = 40*1024 # in the number of HDFS blocks, 1TB = 8*1024 blocks
GAMMA = 0.2  
BALANCE_FACTOR = 1 
NUM_REPLICAS = 2

# YARN
# NODE_LOCALITY_DELAY = NUM_RACK*NUM_HOST_PER_RACK*0.5 # the number of scheduling opportunities since the last container assignment to wait before accepting a placement on another node
RACK_LOCALITY_DELAY = 1 # in second
REDUCE_RAMPUP_LIMIT = 0.2 # fraction of the number of reducers that can be scheduled before all mappers are scheduled
REDUCE_SLOW_START = 0.9
AM_INIT_TIME = 1 # AM container initialization time
NM_HEARTBEAT_INTERVAL = 0.5 # time interval of node manager heartbeats, in second 
CPU_UTIL_UPDATE_MAX = 20

# Global variables
# HDFS
diskAvail = [DISK_SIZE_PER_HOST]*(NUM_RACK*NUM_HOST_PER_RACK) # free disk space per host, normalized by HDFS block size
numSHDBlocks = [0]*(NUM_RACK*NUM_HOST_PER_RACK) # number of HDFS blocks of SHDs stored per host
numSHDBlocksPerRack = dict([(key, 0) for key in range(NUM_RACK)])
rackN0DiskAvail = dict([(key, DISK_SIZE_PER_HOST*NUM_HOST_PER_RACK) for key in range(NUM_N0)]) # number of free disk space per rack in HDFS blocks (N0 racks)
rackRegularDiskAvail = dict([(key, DISK_SIZE_PER_HOST*NUM_HOST_PER_RACK) for key in range(NUM_N0,NUM_RACK)])
# rackRegularDiskAvail = dict([(key, DISK_SIZE_PER_HOST*NUM_HOST_PER_RACK) for key in range(NUM_RACK)]) # racks only store regular datasets, key: rack index, value: number of blocks that can be stored on a rack
# rackShuffleDiskAvail = dict() # racks store at least some part of a shuffle-heavy dataset
diskAvailPerRack = dict([(key, DISK_SIZE_PER_HOST*NUM_HOST_PER_RACK) for key in range(NUM_RACK)])


# YARN
# hostId: h0, ... , h799
# rackId: r0, ... , r19 
# cntrId: c0, ... , c15999

setSHJob = [] # set of currently running shuffle-heavy jobs
jobInfo = dict() # key: jobId, value: missOpp, numMaps, numAllocatedMaps, numReduces, numAllocatedReduce, dict(taskId, cntrId)
setRackSHJob = dict() # key: jobId(SH jobs only), value: dict('m' : preferred set of racks (rackIds) for mappers, 'r' : preferred set of racks for reducers)








