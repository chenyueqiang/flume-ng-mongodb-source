package org.hades.flume.source;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MongoDB_Flume_Source extends AbstractSource implements Configurable, PollableSource {
    private static Logger logger = LoggerFactory.getLogger(MongoDBSource.class);
    private String lastIndex;
    private Map<String, Object> statusJsonMap;
    // DEFAULT
    private static final boolean DEFAULT_AUTHENTICATION_ENABLED = false;
    private static final boolean DEFAULT_USE_ID_FIELD = true;
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 27017;
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_POLL_INTERVAL = 1000;
    private static final String DEFAULT_DB = "events";
    private static final String DEFAULT_COLLECTION = "events";
    private static final String DEFAULT_STATUS_PATH = "/var/lib/flume/mongodbSource.status";
    private MongoDatabase database;
    private List<String> listHosts;
    private MongoClient mongoClient;
    private FindIterable<Document> cursor;
    private DBObject query;
    private MongoCollection<Document> collection;
    // Attribute
    private int port, batchSize, pollInterval;
    private boolean authenticationEnabled, useIdField;
    private String dbName, collectionName, host, username, password, statusPath;

    private ChannelProcessor channelProcessor;

    @Override
    public synchronized void start() {
        logger.info("Starting {}...", getName());


        // if status file is not exists, then create status file. or if it exists, then read it content
        lastIndex = Utils.getString(statusPath);
        channelProcessor = getChannelProcessor();
        mongoClient = MongoDBConnection.getMongoClient(listHosts,port);
        database = mongoClient.getDatabase("local");
        collection = database.getCollection("oplog.rs");
        System.out.println("mongod初始化==================================");
        super.start();

        logger.info("Started {}.", getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        FindIterable<Document> findIterable;
        if (!StringUtils.isBlank(lastIndex)) {
            long tsl = Long.valueOf(lastIndex);
            BsonTimestamp bs = new BsonTimestamp(tsl);
            findIterable = collection.find(Filters.eq("ts", bs));
        } else {
            //-1倒叙,初始化程序时，取最新的ts时间戳，监听mongodb日志，进行过滤，这里的ts尽量做到，服务停止时，存储到文件或者库，获取最新下标
            findIterable = collection.find().sort(new Document("ts", -1));
        }
        Object ts = findIterable.first().get("ts");
        try {
            while (true) {
                FindIterable<Document> documents = (FindIterable<Document>) collection.find(Filters.and(
                        Filters.gt("ts", ts), Filters.or(
                                Filters.eq("op", "i"),
                                Filters.eq("op", "u"),
                                Filters.eq("op", "d"))));
                for (Document document : documents) { //获取数据解析
                    ts = (BsonTimestamp) document.get("ts"); //缓存ts
                    BsonTimestamp bsonTS = (BsonTimestamp) document.get("ts");
                    String[] ns = document.get("ns").toString().split("\\.");
                    Document o = (Document) document.get("o");
                    JSONObject result = new JSONObject();
                    result.put("databaseName", ns[0]);
                    result.put("tableName", ns[1]);
                    result.put("eventType", getEventType(document.getString("op").toLowerCase()));
                    result.put("logfileOffset", ts.toString());
                    result.put("createTime", System.currentTimeMillis());
                    resultRow(o, result, document.getString("op").toLowerCase());
                    channelProcessor.processEvent(EventBuilder.withBody(result.toJSONString(), Charset.forName("UTF-8")));
                    Utils.saveString(String.valueOf(bsonTS.getValue()), statusPath); //将偏移量存入此文件
                }
                // each poll interval (millisecond)
                Thread.sleep(pollInterval);
                return Status.READY;
            }
        } catch (Exception e) {
            logger.warn("Error ", e);
            mongoClient.close();
            return Status.BACKOFF;
        }
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        listHosts = new ArrayList<>();
        logger.info("Configure {}", getName());
        host = context.getString("host", DEFAULT_HOST);
        port = context.getInteger("port", DEFAULT_PORT);
        authenticationEnabled = context.getBoolean("authenticationEnabled", DEFAULT_AUTHENTICATION_ENABLED);
        if (authenticationEnabled) {
            username = context.getString("username");
            password = context.getString("password");
        } else {
            username = "";
            password = "";
        }
        dbName = context.getString("db", DEFAULT_DB);
        collectionName = context.getString("collection", DEFAULT_COLLECTION);
        batchSize = context.getInteger("batchSize", DEFAULT_BATCH_SIZE);
        useIdField = context.getBoolean("useIdField", DEFAULT_USE_ID_FIELD);
        statusPath = context.getString("statusPath", DEFAULT_STATUS_PATH);
        pollInterval = context.getInteger("pollInterval", DEFAULT_POLL_INTERVAL);
        listHosts.add(host);

        statusJsonMap = new LinkedHashMap<String, Object>();
        statusJsonMap.put("SourceName", getName());
        statusJsonMap.put("Host", host);
        statusJsonMap.put("Port", port);
        statusJsonMap.put("DB", dbName);
        statusJsonMap.put("Collection", collectionName);
        statusJsonMap.put("Query", "");
        statusJsonMap.put("LastIndex", "");

        logger.info("MongoDBSource {} context { host:{}, port:{}, authenticationEnabled:{}, username:{}, password:{}, dbName:{}, collectionName:{}, batchSize:{}, useIdField:{} }",
                new Object[]{getName(), host, port, authenticationEnabled, username, password, dbName, collectionName, batchSize, useIdField});

    }

    /**
     * 解析操作类型
     *
     * @param op
     * @return
     */
    private static String getEventType(String op) {
        switch (op) {
            case "i":
                return "insert";
            case "u":
                return "update";
            case "d":
                return "delete";
            default:
                return "other";
        }
    }

    /**
     * 数据解析、格式封装，返回所有insert、update新数据，delete的老数据，做输出为逻辑删除，condition字段为空
     *
     * @return JSONObject
     */
    private static JSONObject resultRow(Document document, JSONObject result, String eventType) {
        JSONObject columns = new JSONObject();// 存放变化后的字段
        result.put("columns", columns);
        result.put("condition", new JSONObject()); // 条件
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            if (entry.getKey().equalsIgnoreCase("_id")) {
                columns.put(entry.getKey(), (entry.getValue()).toString());
                continue;
            }
            columns.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}
