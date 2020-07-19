chester told me to make a distributed key value store


todo: (roughly in order of priority)
* logging [Done]
* error handling [----]
* nodes leaving [----]
* fault tolerance: [----]
	* data replication [----]
	* peer fault detection and recovery [----]
* consistent hashing implementation [----]

notations:
* @todo - todo!
* @conc - the following segment could be rewritten concurrently(†) 
* @rem  - remove, probably for testing
* @note - a point of note or concern

(†) Often this involves using TBB structures in place of currently used STL ones. This frequently is complicated by the fact that thrift defaults to stl map/vector/set for its IDL types), however this can be worked around.

