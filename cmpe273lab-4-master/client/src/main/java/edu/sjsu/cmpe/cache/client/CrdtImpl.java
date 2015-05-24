package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CrdtImpl {

    public ConcurrentHashMap<String, String> writesStatus = new ConcurrentHashMap<String, String>();
    public ConcurrentHashMap<String, String> readStatus = new ConcurrentHashMap<String, String>();
    private ArrayList<DistributedCacheService> caches = new ArrayList<DistributedCacheService>();

    public void add(String serverURL) {
        caches.add(new DistributedCacheService(serverURL, this));
    }


    public void writeToAllNodes(long key, String value) {

        int failures = 0;

        for (DistributedCacheService ser : caches) {
            ser.put(key, value);
        }

        do {

            if (writesStatus.size() >= caches.size()) {

                for (DistributedCacheService cache : caches) {
                    System.out.println("Writing to " + cache.getCacheServerURL() + ": " + writesStatus.get(cache.getCacheServerURL()));
                    if (writesStatus.get(cache.getCacheServerURL()).equalsIgnoreCase("fail"))
                        failures++;
                }

                if (failures > 1) {
                    System.out.println("Too many Failures...Rollback in progress....");
                    for (DistributedCacheService cache : caches) {
                        cache.delete(key);
                    }
                }
                writesStatus.clear();
                break;
            } else {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } while (true);
    }

    public String readFromAllNodes(long key) throws InterruptedException {

        for (DistributedCacheService cache : caches) {
            cache.get(key);
        }
        Set<DistributedCacheService> failedCaches = new HashSet<DistributedCacheService>();
        Set<DistributedCacheService> consistentCaches = new HashSet<DistributedCacheService>(caches);
        consistentCaches.addAll(caches);


        while (true) {
            if (readStatus.size() < 3) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {


                System.out.println(readStatus);
                for (DistributedCacheService server : caches) {

                    if (readStatus.get(server.getCacheServerURL()).equalsIgnoreCase("fail")) {
                        System.out.println("Failure at : " + server.getCacheServerURL());
                        failedCaches.add(server);
                    }
                }

                consistentCaches.removeAll(failedCaches);
                System.out.println("consistent :" + consistentCaches);
                Thread.sleep(500);
                String valueToAdd = null;

                if (failedCaches.size() > 0) {


                    System.out.println("failed caches : " + failedCaches);
                    ArrayList<String> allValues = new ArrayList<String>();
                    ArrayList<DistributedCacheService> allServers = new ArrayList<DistributedCacheService>();

                    for (DistributedCacheService consServ : consistentCaches) {
                        String temp = consServ.getSync(key);
                        allValues.add(temp);
                        allServers.add(allValues.indexOf(temp), consServ);
                    }

                    //get value to store
                    Set<String> unique = new HashSet<String>(allValues);

                    int max = Integer.MIN_VALUE;
                    DistributedCacheService maxServer = null;

                    for (String val : unique) {
                        int currMax = Collections.frequency(allValues, val);
                        if (currMax > max) {
                            max = currMax;
                            valueToAdd = val;

                        }
                    }

                    System.out.println("Making the servers consistent....");

                    for (DistributedCacheService ser : failedCaches) {
                        System.out.println("Correct value for server: " + ser.getCacheServerURL() + " as: " + valueToAdd);
                        ser.putSync(key, valueToAdd);
                    }
                    failedCaches.clear();

                    readStatus.clear();

                    return valueToAdd;
                }
                failedCaches.clear();

            }
        }

    }

}
