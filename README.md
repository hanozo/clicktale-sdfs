Design
1. The cluster has two types of nodes operating in a master-worker pattern: a namenode (the master) and a number of datanodes (workers).
The namenode manages the filesystem namespace. Datanodes are the workhorses of the filesystem.
2. Group Membership is handled by zookeeper. Datanodes joined a pre-defined persistent znode named "sdfs".
3. I'd like to give the illusion that there is only one replica, avoiding a same read (at the same time) on different replicas responded differently.
Hence lagging replica (eventual consistency) is not an option. For that matter replication as to be synchronous.
4. Aside of a simple path validation all other use cases (creating existing file, deleting nonexistent file etc.) which are subject to race condition were deliberately forwarded to FS good care.

Setup
1. ZK and RMQ
2. Create the parent znode /sdfs by running CreateGroup.java
3. Launch few DataNode.java instances. Please provide each instance with a different port number (e.g. in the range: [65112-65200])
4. Run NameNode.java
5. Run CommandDispatcher.java
6. Run CommandHttpServer.java