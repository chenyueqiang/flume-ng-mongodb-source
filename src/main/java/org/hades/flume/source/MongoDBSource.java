package org.hades.flume.source;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.apache.flume.Context;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.PollableSource;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.channel.ChannelProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.JSONException;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * 这个方式只适用于单主机操作mongodb数据，
 * 切勿使用在多节点操作mongodb的场景。
 * 会丢数据
 */
public class MongoDBSource extends AbstractSource implements Configurable, PollableSource {
    private static Logger logger = LoggerFactory.getLogger(MongoDBSource.class);

    private File statusFile;
    private String lastIndex = "";
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

    // Attribute
    private int port, batchSize, pollInterval;
    private boolean authenticationEnabled, useIdField;
    private String dbName, collectionName, host, username, password, statusPath;

    private ChannelProcessor channelProcessor;

    // Get conf configuration information
    @Override
    public void configure(Context context) {
        listHosts=new ArrayList<>();
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

    @Override
    public synchronized void start() {
        logger.info("Starting {}...", getName());

        // if status file is not exists, then create status file. or if it exists, then read it content
        readyStatus(true);

        channelProcessor = getChannelProcessor();
        mongoClient = MongoDB_SyncData.getMongoClient(username, password, listHosts, dbName, port);
        database = mongoClient.getDatabase(dbName);
        System.out.println("mongod初始化==================================");
        super.start();

        logger.info("Started {}.", getName());
    }

    @Override
    public Status process() throws EventDeliveryException {

        try {
            if (lastIndex.equals("")) {
                cursor = database.getCollection(collectionName).find().limit(batchSize);
            } else {
                BasicDBObject q = new BasicDBObject("_id", new BasicDBObject("$gt", new ObjectId(lastIndex)));
                cursor = database.getCollection(collectionName).find(q).limit(batchSize);
            }

            MongoCursor<Document> iterator = cursor.iterator();

            // read result
            while (iterator.hasNext()) {
                Document next = iterator.next();
                String result = getResult(next);
                channelProcessor.processEvent(EventBuilder.withBody(result, Charset.forName("UTF-8")));
                lastIndex = next.get("_id").toString();
            }

            // refresh status after each read result
            refreshStatus();

            // each poll interval (millisecond)
            Thread.sleep(pollInterval);

            return Status.READY;
        } catch (Exception e) {
            logger.warn("Error ", e);
            mongoClient.close();
            return Status.BACKOFF;
        }

    }

    //2.source投送到channel，channel反馈已满载（投递失败），需要等待挂起，设置2s
    public long getBackOffSleepIncrement() {
        return 2000;
    }

    //3.投递失败，最大等待时间
    public long getMaxBackOffSleepInterval() {
        return 10000;
    }

    @Override
    public void stop() {
        logger.info("Stopping {}.", getName());
        try {
            mongoClient.close();
            channelProcessor.close();
        } catch (Exception e) {
            logger.warn("Error ", e);
        } finally {
            super.stop();
        }
        logger.info("Stopped {}.", getName());
    }

    @SuppressWarnings("unchecked")
    private void readyStatus(boolean op) {
        if (op) {
            statusFile = new File(statusPath);
        } else {
            // if op is false, it means is content of status file is not formatted correctly, then add ".new" to statusPath.
            statusPath = statusPath + ".new";
            statusFile = new File(statusPath);
        }
        if (!statusFile.exists() || (statusFile.exists() && statusFile.isDirectory())) {
            // if status file not exists, then create status file and write content, but lastIndex=""
            lastIndex = "";

            String statusPreDirPath = statusPath.substring(0, statusPath.lastIndexOf("/"));
            File statusDir = new File(statusPreDirPath);
            try {
                if (!statusDir.exists() || (statusDir.exists() && !statusDir.isDirectory())) {
                    if (!statusDir.mkdirs()) {
                        logger.warn("Failed of create directory, please confirm the path is correct or you have permissions.");
                    }
                }
                if (statusFile.createNewFile()) {
                    try {
                        FileWriter fw = new FileWriter(statusFile, false);
                        JSONValue.writeJSONString(statusJsonMap, fw);
                        fw.close();
                    } catch (IOException e) {
                        logger.error("Error creating value for status file, please confirm the path is correct or you have permissions. ", e);
                    }
                } else {
                    logger.warn("Failed of create file, please confirm the path is correct or you have permissions.");
                }
            } catch (IOException e) {
                logger.error("Error IOException ", e);
            }
        } else {
            // if status file exists, then read status file and refresh lastIndex
            try {
                FileReader fileReader = new FileReader(statusFile);
                JSONParser jsonParser = new JSONParser();
                statusJsonMap = (Map) jsonParser.parse(fileReader);
                lastIndex = statusJsonMap.get("LastIndex").toString();
            } catch (FileNotFoundException e) {
                logger.warn("File Not Found!!! ", e);
                readyStatus(true);
            } catch (JSONException e) {
                logger.error("Exception of JSON ", e);
                readyStatus(false);
            } catch (Exception e) {
                logger.error("Exception reading status file, doing back up and creating new status file", e);
                readyStatus(false);
            }
        }
    }

    // refresh status file
    private void refreshStatus() {
        statusJsonMap.put("LastIndex", lastIndex);
        statusJsonMap.put("Query", query.toString());

        try {
            FileWriter fw = new FileWriter(statusFile, false);
            JSONValue.writeJSONString(statusJsonMap, fw);
            fw.close();
        } catch (IOException e) {
            logger.error("Error creating value for status file, please confirm the path is correct or you have permissions. ", e);
        }
    }

    // get select result
    private String getResult(DBObject resultObject) {
        if (!useIdField) {
            JSONObject jsonObject = new JSONObject();
            for (String key : resultObject.keySet()) {
                if (!key.equals("_id")) {
                    jsonObject.put(key, resultObject.get(key));
                }
            }
            return jsonObject.toString();
        } else {
            return resultObject.toString();
        }
    }

    private String getResult(Document next) {
        if (!useIdField) {
            JSONObject jsonObject = new JSONObject();
            for (String key : next.keySet()) {
                if (!key.equals("_id")) {
                    jsonObject.put(key, next.get(key));
                }
            }
            return jsonObject.toString();
        } else {
            return next.toString();
        }
    }

}
