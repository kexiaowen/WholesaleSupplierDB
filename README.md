# WholesaleSupplierDB
CS4224 Distributed Databases Project - Team 1

1. Installing and Running Cassandra
    1. Install Cassandra on the cluster of machines from binary tarball files. Extract the files into extracted into `apache-cassandra-3.11.3`.

            > cd /temp 

            > wget http://www-us.apache.org/dist/cassandra/3.11.3/apache-cassandra-3.11.3-bin.tar.gz

    2. Configure Cassandra by editing `/temp/apache-cassandra-3.11.3/conf/cassandra.yaml` for all nodes in the cluster. Run `hostname -i` to find out the IP address of the node. Set **listen address** as the IP address of the node. Choose one node in the cluster to be the seed node. Set the **seed** for all nodes in the cluster as the IP address of the seed node.
    
    For examples, 
    ![seed](https://github.com/kexiaowen/WholesaleSupplierDB/blob/master/Images/seeds.png)
    
    ![listen_address](https://github.com/kexiaowen/WholesaleSupplierDB/blob/master/Images/listen_address.png)

    3. On every node in the cluster, starting from the seed node, start Cassandra by invoking the command below:
    
            > /temp/apache-cassandra-3.11.3/bin/cassandra

    4. Verify that Cassandra is running on all nodes in the cluster by invoking the following command:
    
            > /temp/apache-cassandra-3.11.3/bin/nodetool status
            
       You should be able to see the following:
       ![nodetool_status](https://github.com/kexiaowen/WholesaleSupplierDB/blob/master/Images/verifyNodeStatus.png)


2. Installing Maven for Building Source Code
    1. Download Maven using the following command:
        
            > cd ~

            > wget http://www-us.apache.org/dist/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz

    2. Extract Maven distribution archive using the following command:

            > tar xzf apache-maven-3.5.4-bin.tar.gz

3. Setting up and Building
    1. Put the code files WholesaleSupplierDB folder at `~/WholesaleSupplierDB`

    2. Run the following command to build a JAR file from source code
 
            > cd WholesaleSupplierDB
            
            > ~/apache-maven-3.5.4/bin/mvn package

    3. The jar file built is found in `~/WholesaleSupplierDB/target`

4. Loading Data into Cassandra
    1. Place project data files at directory `~/4224-project-files` so that data files are found in `~/4224-project-files/data-files` and transaction files are found in `~/4224-project-files/xact-files`.
    2. Execute shell script `WholesaleSupplier/loadDataAndCreateTables.sh` by running the following commands:
    
            > cd WholesaleSupplierDB
            
            > sed -i 's/\r$//' loadDataAndCreateTables.sh
            
            > chmod +x loadDataAndCreateTables.sh
            
            > ./loadDataAndCreateTables.sh  

5. Running Different Transactions Using the Client Driver

            > cd WholesaleSupplierDB

            > ~/apache-maven-3.5.4/bin/mvn exec:java -Dexec.args="[ip_address] [consistency_level]" < [input_file_name]
            
            Note:
            * The consistency must either be ONE or QUORUM
            * No spacing between `-Dexec.args` and `=`, but the spacing is required between 2 arguments
            * Through our test, the node address (192.168.48.219) does not work, but localhost (127.0.0.1) works.

6. After finishing running, press `Ctrl+C` to stop the process
            
