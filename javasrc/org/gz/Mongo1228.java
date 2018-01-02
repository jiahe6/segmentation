package org.gz;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//基本标签二轮迭代主函数入口20170721

import org.apache.log4j.Logger;
import org.bson.Document;
import org.gz.mongospark.SegWithOrigin2;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import tp.test.CourtTest;
import tp.util.FileCharsetDetector;

public class Mongo1228 {
	
	static MongoClientURI mongoURI = new MongoClientURI("mongodb://yuanyuan:123456@192.168.12.161:27017,192.168.12.162:27017,192.168.12.163:27017,192.168.12.164:27017,192.168.12.169:27017,192.168.12.162:27017/?authSource=datamining");
	static MongoClient mongoClient = new MongoClient(mongoURI);
	static MongoDatabase mongoDatabase_all = mongoClient.getDatabase("wenshu");
	static MongoCollection mcoll_all = mongoDatabase_all.getCollection("origin2");
	static MongoDatabase mongoDatabase_tiny = mongoClient.getDatabase("datamining");
	static MongoCollection mcoll_online_tiny = mongoDatabase_tiny.getCollection("onlinedata");
	static MongoCollection mcoll_court_tiny = mongoDatabase_tiny.getCollection("court");
	static MongoCollection mcoll_gongbao_tiny = mongoDatabase_tiny.getCollection("gongbao_anli");
	static Map<String,Integer> count_map=new HashMap<String,Integer>();
//	static String[] seg_array={};
	static Document doc = new Document();
	final static Logger LOG = Logger.getLogger(Mongo1228.class);
	
	// 单条记录插入
	public static void WriteDataOnce(Document doc) {
		LOG.debug("入库之前");
		try {
//			 mcoll.insertOne(doc);
//			mcoll_courtNull.insertOne(doc);
//			mcoll_casetype_tiny.insertOne(doc);
		} catch (Exception e) {
			LOG.info("入库报错");
			e.printStackTrace();
		}
		LOG.debug("入库之后");
	}
	// 单条记录插入
	// 单条记录修改
	public static void updateDataOnce(Document old_doc, Document new_doc) {
		LOG.debug("更新之前");
		try {
//			mcoll_test_tiny.updateOne(Filters.eq("_id", old_doc.get("_id").toString()), new Document("$set", new_doc));
//			mcoll_court_tiny.updateOne(Filters.eq("_id", old_doc.get("_id").toString()), new Document("$set", new_doc));
//			mcoll.updateOne(Filters.eq("_id", old_doc.get("_id").toString()), new Document("$set", new_doc));
//			mcoll_laodong.updateOne(Filters.eq("_id", old_doc.get("_id").toString()), new Document("$set", new_doc));
//			mcoll_lihun_test.updateOne(Filters.eq("_id", old_doc.get("_id").toString()), new Document("$set", new_doc));
//			mcoll_lihun.updateOne(Filters.eq("_id", old_doc.get("_id").toString()), new Document("$set", new_doc));
		} catch (Exception e) {
			LOG.info("更新报错");
			e.printStackTrace();
		}
		LOG.debug("更新之后");
	}
	
		// 单条记录修改
				public static void updateDataOnce(Document old_doc, Document new_doc,MongoCollection mcoll) {
					LOG.debug("更新之前");
					try {
						mcoll.updateOne(Filters.eq("_id", old_doc.get("_id").toString()), new Document("$set", new_doc));
					} catch (Exception e) {
						LOG.info("更新报错");
						e.printStackTrace();
					}
					LOG.debug("更新之后");
				}
	
	// 遍历全部数据，并调用基本标签抽取方法
		public static void getDoubleData(MongoCollection mcoll) {
//			String txt="";
			int count = 0;
			// 遍历全部数据
			FindIterable<Document> findIterable = mcoll.find().noCursorTimeout(true);
			MongoCursor<Document> mongoCursor = findIterable.iterator();
			while (mongoCursor.hasNext()) {
				count++;
				if (count % 10000 == 0) {
					LOG.info("court_count:" + count);
				}
				Document obj = mongoCursor.next();
//					LOG.debug(obj.get("_id").toString());
					// 调用基本标签抽取方法
					if(obj.get("segdata")!=null){
						Document obj_seg=(Document) obj.get("segdata");
						if(obj_seg.get("全文")!=null){
							ArrayList<Document> txt_array = (ArrayList<Document>) obj_seg.get("全文");
							for(Document d:txt_array){
								String key=d.keySet().iterator().next();
//								System.out.println(key);
								if(!count_map.containsKey(key)){
									count_map.put(key, 1);
								}else{
//									遇到有重复段落
//									txt+=obj.get("_id").toString()+"-->【"+key+"】\r\n";
									count_map.clear();
									Document seg = SegWithOrigin2.segment(obj.getString("content").split("\n"));
									System.out.println(seg.toJson());
									break;
								}
							}
						}
					}
					count_map.clear();
			}
//			System.out.println("=======================");
//			System.out.println(txt);
		}
	public static void main(String[] args) throws FileNotFoundException, IOException {
		getDoubleData(mcoll_gongbao_tiny);
		mongoClient.close();
	}
}
