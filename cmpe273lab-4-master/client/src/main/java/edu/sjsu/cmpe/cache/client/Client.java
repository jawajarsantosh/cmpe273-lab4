package edu.sjsu.cmpe.cache.client;

/**
 * Client
 */

public class Client {

    public static void main(String[] args) throws Exception {


        System.out.println("Starting Cache Client...");

        CrdtImpl CRDT = new CrdtImpl();
        CRDT.add("http://localhost:3000");
        CRDT.add("http://localhost:3001");
        CRDT.add("http://localhost:3002");


        //PUT key-value pair into all three nodes
        CRDT.writeToAllNodes(1, "a");

	//sleep the thread for 30 secs
        Thread.sleep(30 * 1000);

	//Update key-value pair in all three nodes
	CRDT.writeToAllNodes(1, "b");

	//sleep the thread for 30 secs
        Thread.sleep(30 * 1000);

        System.out.println("Updated value in all the three servers: " + crdt.readFromAllNodes(1));


        //Test for Write Rollback
        Thread.sleep(30 * 1000);
        CRDT.writeToAllNodes(2, "c");


        System.out.println("Exiting Cache Client.");



    }

}

