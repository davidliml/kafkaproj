package com.jasongj.kafka.stream;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import com.jasongj.kafka.stream.model.Item;
import com.jasongj.kafka.stream.model.Order;
import com.jasongj.kafka.stream.model.User;
import com.jasongj.kafka.stream.serdes.SerdesFactory;
import com.jasongj.kafka.stream.timeextractor.OrderTimestampExtractor;

public class PurchaseAnalysis {

	public static void main(String[] args) throws IOException, InterruptedException {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-purchase-analysis2");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.143:9092");
		props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "192.168.100.143:2181");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimestampExtractor.class);



		
		
		
		
		KStreamBuilder streamBuilder = new KStreamBuilder();
		KStream<String, Order> orderStream = streamBuilder.stream(Serdes.String(), SerdesFactory.serdFrom(Order.class), "orders");
		KTable<String, User> userTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(User.class), "users", "users-state-store");
		KTable<String, Item> itemTable = streamBuilder.table(Serdes.String(), SerdesFactory.serdFrom(Item.class), "items", "items-state-store");

		KTable<String, String> kTable = orderStream
				.leftJoin(itemTable, (Order order, Item item) -> OrderItem.fromOrder(order, item), Serdes.String(), SerdesFactory.serdFrom(Order.class))
				.filter((String itemAddress, OrderItem orderUser) -> orderUser.itemAddress != null)
				.map((String itemName, OrderItem orderItem) -> new KeyValue<String, OrderItem>(orderItem.itemName, orderItem))
.through(Serdes.String(), SerdesFactory.serdFrom(OrderItem.class), (String key, OrderItem orderUser, int numPartitions) -> (orderUser.getItemName().hashCode() & 0x7FFFFFFF) % numPartitions, "orderuser-repartition-by-item")
			.map((String item, OrderItem orderItem) -> KeyValue.<String, String>pair(orderItem.itemType +","+ orderItem.itemName+","+ orderItem.itemPrice,itemGenOutStr(orderItem)))// (Double)(orderUserItem.quantity * orderUserItem.itemPrice)))
				.groupByKey(Serdes.String(), Serdes.String())
				.reduce((String v1, String v2) -> sumall(v1,v2), "gender-amount-state-store");
		
		
		
		//	输出字段包含时间窗口（起始时间，结束时间），品类（category），商品名（item_name），销量（quantity），单价（price），总销售额，该商品在该品类内的销售额排名
		//1、定单商品按item_name合并 按   品类（category），商品名（item_name）、单价（price）为key 汇总 数量*单价
		
		
		//定单 user_name, item_name, transaction_ts, quantity  
		    //Jack, iphone, 2016-11-11 00:00:01, 3
		//商品item_name, item_address, category, price 
	        //iphone, BJ, phone, 5388.88
		//用户 Jack, BJ, male, 23
		
		
		System.out.printf("时间窗口（起始时间，结束时间），品类（category），商品名（item_name），销量（quantity），单价（price），总销售额，该商品在该品类内的销售额排名\n");
		kTable.foreach((str, dou) -> System.out.printf("%s,%s\n", str, dou));
		kTable
			.toStream()
			.map((String gender, String total) -> new KeyValue<String, String>(gender, String.valueOf(total)))
			.to("gender-amount");
		
		KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder, props);
		kafkaStreams.cleanUp();
		kafkaStreams.start();
		
		System.in.read();
		kafkaStreams.close();
		kafkaStreams.cleanUp();
	}
	
	private Object[] genDouByStr(String str) {
		String[] v = str.split(",");
		Object[] ret = new Object[3];
		for (int i =0;i<3;i++) {
			String dv =v[i];
			ret[i] = Double.valueOf(dv);
		}
		return ret;
	}
	
	private static String itemGenOutStr( OrderItem orderItem) {
		int ordN = 1;
		int spn = orderItem.quantity;
		Double sumD =(Double)(orderItem.quantity * orderItem.itemPrice); 
	//	System.out.println(ordN+"_"+spn+"_"+String.valueOf(sumD));
		return ordN+"_"+spn+"_"+String.valueOf(sumD);
	}
	
	private static String sumall(String src,String src2) {
		String[] srcs = src.split("_");
		
		
		String[] src2s = src2.split("_");
		//System.out.println(src+"*"+src2);
		if (srcs==null||srcs.length == 1)
		{
			 int ordn = Integer.parseInt(src2s[0]);
			    int spn = Integer.parseInt(src2s[1]);
			    Double sumD = Double.parseDouble(src2s[2]);
			    return ordn+","+spn+","+String.valueOf(sumD); 
		}
		
	    int ordn = Integer.parseInt(srcs[0])+Integer.parseInt(src2s[0]);
	    int spn = Integer.parseInt(srcs[1])+Integer.parseInt(src2s[1]);
	    Double sumD = Double.parseDouble(srcs[2])+Double.parseDouble(src2s[2]);
	    return ordn+","+spn+","+String.valueOf(sumD);
		
	}
	
	
	private String comStrByDouble(Double[] d) {
		String ret = "";
		for (int i =0;i<3;i++) {
			String dv =String.valueOf(d[i]);
			ret =ret+","+dv;
		}
		return ret.substring(1);
		
		
		
	}
	
	
	public static class OrderUserItem {
		private String userName;
		private String itemName;
		private long transactionDate;
		private int quantity;
		private String userAddress;
		private String gender;
		private int age;
		private String itemAddress;
		private String itemType;
		private double itemPrice;
		
		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public String getItemName() {
			return itemName;
		}

		public void setItemName(String itemName) {
			this.itemName = itemName;
		}

		public long getTransactionDate() {
			return transactionDate;
		}

		public void setTransactionDate(long transactionDate) {
			this.transactionDate = transactionDate;
		}

		public int getQuantity() {
			return quantity;
		}

		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}

		public String getUserAddress() {
			return userAddress;
		}

		public void setUserAddress(String userAddress) {
			this.userAddress = userAddress;
		}

		public String getGender() {
			return gender;
		}

		public void setGender(String gender) {
			this.gender = gender;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String getItemAddress() {
			return itemAddress;
		}

		public void setItemAddress(String itemAddress) {
			this.itemAddress = itemAddress;
		}

		public String getItemType() {
			return itemType;
		}

		public void setItemType(String itemType) {
			this.itemType = itemType;
		}

		public double getItemPrice() {
			return itemPrice;
		}

		public void setItemPrice(double itemPrice) {
			this.itemPrice = itemPrice;
		}

		public static OrderUserItem fromOrderUser(OrderUser orderUser) {
			OrderUserItem orderUserItem = new OrderUserItem();
			if(orderUser == null) {
				return orderUserItem;
			}
			orderUserItem.userName = orderUser.userName;
			orderUserItem.itemName = orderUser.itemName;
			orderUserItem.transactionDate = orderUser.transactionDate;
			orderUserItem.quantity = orderUser.quantity;
			orderUserItem.userAddress = orderUser.userAddress;
			orderUserItem.gender = orderUser.gender;
			orderUserItem.age = orderUser.age;
			return orderUserItem;
		}

		public static OrderUserItem fromOrderUser(OrderUser orderUser, Item item) {
			OrderUserItem orderUserItem = fromOrderUser(orderUser);
			if(item == null) {
				return orderUserItem;
			}
			orderUserItem.itemAddress = item.getAddress();
			orderUserItem.itemType = item.getType();
			orderUserItem.itemPrice = item.getPrice();
			return orderUserItem;
		}
	}

	
	public static class OrderUser {
		private String userName;
		private String itemName;
		private long transactionDate;
		private int quantity;
		private String userAddress;
		private String gender;
		private int age;
		
		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public String getItemName() {
			return itemName;
		}

		public void setItemName(String itemName) {
			this.itemName = itemName;
		}

		public long getTransactionDate() {
			return transactionDate;
		}

		public void setTransactionDate(long transactionDate) {
			this.transactionDate = transactionDate;
		}

		public int getQuantity() {
			return quantity;
		}

		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}

		public String getUserAddress() {
			return userAddress;
		}

		public void setUserAddress(String userAddress) {
			this.userAddress = userAddress;
		}

		public String getGender() {
			return gender;
		}

		public void setGender(String gender) {
			this.gender = gender;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public static OrderUser fromOrder(Order order) {
			OrderUser orderUser = new OrderUser();
			if(order == null) {
				return orderUser;
			}
			orderUser.userName = order.getUserName();
			orderUser.itemName = order.getItemName();
			orderUser.transactionDate = order.getTransactionDate();
			orderUser.quantity = order.getQuantity();
			return orderUser;
		}
		
		public static OrderUser fromOrderUser(Order order, User user) {
			OrderUser orderUser = fromOrder(order);
			if(user == null) {
				return orderUser;
			}
			orderUser.gender = user.getGender();
			orderUser.age = user.getAge();
			orderUser.userAddress = user.getAddress();
			return orderUser;
		}
	}
	
	//定单 user_name, item_name, transaction_ts, quantity  
    //Jack, iphone, 2016-11-11 00:00:01, 3
//商品item_name, item_address, category, price
	
	public static class OrderItem {
		
		private String itemName;
		private long transactionDate;
		private int quantity;		
		private String itemAddress;
		private String itemType;
		private double itemPrice;
		
		public String getItemName() {
			return itemName;
		}

		public void setItemName(String itemName) {
			this.itemName = itemName;
		}

		public long getTransactionDate() {
			return transactionDate;
		}

		public void setTransactionDate(long transactionDate) {
			this.transactionDate = transactionDate;
		}

		public int getQuantity() {
			return quantity;
		}

		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}

		

		public String getItemAddress() {
			return itemAddress;
		}

		public void setItemAddress(String itemAddress) {
			this.itemAddress = itemAddress;
		}

		public String getItemType() {
			return itemType;
		}

		public void setItemType(String itemType) {
			this.itemType = itemType;
		}

		public double getItemPrice() {
			return itemPrice;
		}

		public void setItemPrice(double itemPrice) {
			this.itemPrice = itemPrice;
		}

		public static OrderItem fromOrder(Order order) {
			OrderItem orderItem = new OrderItem();
			if(order == null) {
				return orderItem;
			}
			
			orderItem.itemName = order.getItemName();
			orderItem.transactionDate = order.getTransactionDate();
			orderItem.quantity = order.getQuantity();
			
			
			return orderItem;
		}

		public static OrderItem fromOrder(Order order, Item item) {
			OrderItem orderItem = fromOrder(order);
			if(item == null) {
				return orderItem;
			}
			orderItem.itemAddress = item.getAddress();
			orderItem.itemType = item.getType();
			orderItem.itemPrice = item.getPrice();
			return orderItem;
		}
	}

}
