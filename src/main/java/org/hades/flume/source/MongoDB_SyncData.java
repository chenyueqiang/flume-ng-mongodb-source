package org.hades.flume.source;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MongoDB_SyncData {
    public static MongoClient getMongoClient(String userName, String password, List<String> hosts, String dataBase,  int port) {
        MongoClient mongoClient = null;

        MongoCredential credential = MongoCredential.
                createScramSha1Credential(userName, dataBase, password.toCharArray());
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.connectionsPerHost(20); //与目标库能够建立的最大连接数
        builder.maxWaitTime(1000 * 60 * 1);//等待时长
        builder.connectTimeout(1000 * 60);//连接超时时长
        MongoClientOptions mongoClientOptions = builder.build();
        List<ServerAddress> serverList = new ArrayList<>(); //mongodb集群地址
        if (hosts != null && hosts.size() > 0) {
            for (String host : hosts
            ) {
                serverList.add(new ServerAddress(host, port));
            }
        } else {
            throw new RuntimeException("请输入MongoDB的连接地址！！！！！！");
        }

        mongoClient = new MongoClient(serverList, Collections.singletonList(credential), mongoClientOptions);
        return mongoClient;
    }


        /**
         * 获取数据库
         *
         * @return
         */
    public static MongoDatabase getDatabase(String userName, String password, List<String> hosts, String dataBase,  int port) {
        MongoDatabase database = null;
        MongoClient mongoClient = getMongoClient(userName, password, hosts, dataBase, port);
        database = mongoClient.getDatabase(dataBase);
        return database;
    }
    //flume-ng agent -c . -f /opt/workhome/flume_project/conf/avro.conf -n agent -Dflume.root.logger=INFO,console
}
