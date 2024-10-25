# HyDFS
Hybrid Distributed File System built for CS 425 MP3

---
## Run
Go to ```src``` folder

    go run . {n}

where n is the machine id from 1 to 10.

## Design
1. The total number of servers is expected to be not larger than 10.
2. All servers use a pre-determined consistent hashing to map servers and files to points on a ring.
3. A file is replicated on the first *n* successor servers in the ring.
4. Each server maintains a full membership list (based on failure detection). Given a filename, a server can route the request to one of the replicas in *O(1)* time.
5. Supported file size should be at least 100s of MBs.
6. Client-side caching. Cache the **file reads** only.

## Tolerance
1. Data stored in HyDFS is tolerant of up to two *simultaneous* machine failures. 
2. A pull-based re-replication is applied (each node periodically checks if its n predecessors has changed).

## Consistency
1. Appends are eventually applied in the same order across the replicas of a file (eventual consistency).
2. Two appends from the same client should be applied in order.
3. ```get``` operation should return the latest appends that the same client performed (not necessarily reflecting others').

## Allowed File Operations
1. ```create localfilename HyDFSfilename``` to create a file on HyDFS being a copy of the local file. Only the first time creation should be accepted.
2. ```get HyDFSfilename localfilename``` to fetch file from HyDFS to local.
3. ```append localfilename HyDFSfilename``` appends the content to HyDFS file, it requires the destination file to be already exist.
4. ```merge HyDFSfilename``` once merge completes, all replicas of a file are identical (**assume that no concurrent updates/failures happen during merge**). An immediate call of merge after a previous merge should return immediately.
5. Testing purpose: ```ls HyDFSfilename``` lists all machine (VM in the test case) addresses and IDs on the ring where this file is currently being stored.
6. On server, testing purpose: ```store``` lists all files together with their IDs on the ring currently stored at this machine. Also, print the machine's ID on the ring.
7. Testing purpose: ```getfromreplica VMaddress HyDFSfilename localfilename``` performs get but from the machine specified by the address.
8. On server, testing purpose: ```list_mem_ids``` lists the membership and the IDs on the ring corresponding to the nodes.
