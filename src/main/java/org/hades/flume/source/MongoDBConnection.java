package org.hades.flume.source;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoDBConnection {

    public static MongoDatabase getDataBase(MongoClient mongoClient,String databaseName) {
        MongoDatabase mgdb = mongoClient.getDatabase(databaseName);
        return mgdb;
    }
    public static MongoClient getMongoClient(List<String> hosts,int port){
        List<ServerAddress> serverList = new ArrayList<>(); //mongodb集群地址
        if(hosts!=null&&hosts.size()>0){
            for (String host:hosts
            ) {
                serverList.add(new ServerAddress(host,port));
            }
        }else {
            throw new RuntimeException("主机名为空！！！！");
        }
        // To connect to mongodb server
        MongoClient mongoClient = new MongoClient(serverList);
        return mongoClient;
    }

    /**
     * 获取表对象
     *
     * @param mongoDatabase
     * @param collectionName
     * @return
     */
    public static MongoCollection<Document> getCollection(MongoDatabase mongoDatabase, String collectionName) {
        MongoCollection<Document> collection = null;
        try {
            //获取数据库dataBase下的集合collecTion，如果没有将自动创建
            collection = mongoDatabase.getCollection(collectionName);
        } catch (Exception e) {
            throw new RuntimeException("获取" + mongoDatabase.getName() + "数据库下的" + collectionName + "集合 failed !" + e);
        }
        return collection;
    }
}
