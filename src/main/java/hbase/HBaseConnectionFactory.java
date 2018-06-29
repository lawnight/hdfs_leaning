package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class HBaseConnectionFactory {
    private static final Logger logger = LoggerFactory.getLogger(HBaseConnectionFactory.class);

    private static Configuration config;
    private static Connection connection = null;

    static {
        try {
            Properties prop = new Properties();
            try (InputStream in = HBaseConnectionFactory.class.getResourceAsStream("/hbase.properties")) {
                prop.load(in);
            }
            logger.info("properties: " + prop.toString());
            //Properties
            // String zkList = "210.73.210.66:2181 210.73.210.67:2181 210.73.210.71:2181 210.73.210.104:2181 210.73.210.105:2181";
            // String zkList = "192.168.4.66:2181 192.168.4.67:2181 192.168.4.71:2181 192.168.6.104:2181 192.168.6.105:2181";
            //String zkList = "bigdata-node-1:2181 bigdata-node-2:2181 bigdata-node-3:2181 bigdata-node-4:2181 bigdata-node-5:2181";
            String zkList = prop.getProperty(HConstants.ZOOKEEPER_QUORUM);

            logger.info("{}={}", HConstants.ZOOKEEPER_QUORUM, zkList);

            Configuration that = new Configuration();
            that.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            that.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

            config = HBaseConfiguration.create(that);
            config.set(HConstants.ZOOKEEPER_QUORUM, zkList);
            config.setInt(HConstants.HBASE_CLIENT_SCANNER_CACHING, 10000); // 客户端扫描仪缓存
            config.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 128);
            // config.setInt(HConstants.HBASE_CLIENT_PAUSE, 1000);
            config.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 10 * 60 * 1000L);
            // config.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, 10 * 60 * 1000L);
            config.setLong(HConstants.HBASE_RPC_TIMEOUT_KEY, 10 * 60 * 1000L);

            connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            logger.error("IOException", e);
            System.exit(1);
        }
    }

    public static Connection getConnection() throws IOException {
        return connection;
    }

    public static void reconnect() throws IOException {
        try {
            connection.close();
        } catch (IOException e) {
            logger.error("IOException", e);
        }
        connection = ConnectionFactory.createConnection(config);
    }

    public static void close() throws IOException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

//    public static void main(String[] args) throws IOException {
//        StringBuilder sb = new StringBuilder(2048);
//        try (RegionLocator locate = connection.getRegionLocator(TableName.valueOf("log:raw_data"))) {
//            for (HRegionLocation hlocate : locate.getAllRegionLocations()) {
//                String host = hlocate.getHostnamePort();
//
//                HRegionInfo hinfo = hlocate.getRegionInfo();
//                String startKey = Bytes.toString(hinfo.getStartKey());
//                String endKey = Bytes.toString(hinfo.getEndKey());
//
//                sb.setLength(0);
//                sb.append("host: [").append(host).append("], ");
//                sb.append("startKey: [").append(startKey).append("], ");
//                sb.append("endKey: [").append(endKey).append("]\n");
//
//                sb.append(hlocate.toString()).append("\n");
//                sb.append(hinfo.toString()).append("\n");
//                System.out.println(sb.toString());
//            }
//        }
//    }
}
