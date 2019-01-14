Naming conventions:
1. constant: all-uppercase words separated by underscores 






Rules to select reduce racks: 
i) racks with shortest queue lengths
ii) racks with shortest queue lengths + available circuits





Moving map input/ reduce input of SHJ: 
	node-local and rack-local cases: shuffle time = inputSize/RATE_INTRA_RACK


Shuffling uses optical circuits:
One slot in the reservation window is 10 ms.
The earliest start time of a circuit request is the very next slot of the slot within which the request arrives.
i) single reduce rack: two choice — a) one circuit directly through the OCS; 
b) first push the shuffle data to the storage and then pull down
If finishTime_a <= finishTime_b, then choose a), else choose b)
The data pulling down must be delayed by one slot compared to the data pushing up.
When calculating finishTime_b, it is chosen to be the smallest value among all the
possible combination of OCS-core links and core-storage links

ii) more than one reduce rack
a) complete multicast without storing
b) store-and-forward
	 

Assumptions:
i) storage size is large enough for temporarily storing shuffling data when needed





















