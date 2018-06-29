package hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class HBaseHelper {
    private static final Logger logger = LoggerFactory.getLogger(HBaseHelper.class);

    public static List<NamespaceDescriptor> listNamespace() throws IOException {
        List<NamespaceDescriptor> ret = new LinkedList<>();
        try (Admin admin = HBaseConnectionFactory.getConnection().getAdmin()) {
            NamespaceDescriptor[] nsds = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor nsd : nsds) {
                ret.add(nsd);
            }
        }
        return ret;
    }

    public static boolean namespaceExist(String ns) throws IOException {
        boolean ret = false;
        List<NamespaceDescriptor> nsds = listNamespace();
        for (NamespaceDescriptor nsd : nsds) {
            if (nsd.getName().equals(ns)) {
                ret = true;
                break;
            }
        }
        return ret;
    }

    public static List<HTableDescriptor> listTableByNamespace(String ns) throws IOException {
        List<HTableDescriptor> ret = new LinkedList<>();
        try (Admin admin = HBaseConnectionFactory.getConnection().getAdmin()) {
            if (namespaceExist(ns)) {
                HTableDescriptor[] hds = admin.listTableDescriptorsByNamespace(ns);
                for (HTableDescriptor hd : hds) {
                    ret.add(hd);
                }
            }
        }
        return ret;
    }

    public static void createNamespace(String ns) throws IOException {
        try (Admin admin = HBaseConnectionFactory.getConnection().getAdmin()) {
            if (!namespaceExist(ns)) {
                NamespaceDescriptor nsd = NamespaceDescriptor.create(ns).build();
                admin.createNamespace(nsd);
            }
        }
    }

    public static void createTableWithSnappyCompress(String tableFullName, String familyName) throws IOException {

        if (tableFullName.contains(":")) { // 创建不存在的namespace
            String[] tokens = tableFullName.split(":");
            String namespace = tokens[0];
            createNamespace(namespace);
        }

        try (Admin admin = HBaseConnectionFactory.getConnection().getAdmin()) {
            TableName tn = TableName.valueOf(tableFullName);
            if (!admin.tableExists(tn)) {
                HTableDescriptor table = new HTableDescriptor(tn);
                table.setCompactionEnabled(true);

                HColumnDescriptor hcd = new HColumnDescriptor(familyName); // familyName: f1
                hcd.setMaxVersions(1);
                hcd.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
                hcd.setCompressionType(Compression.Algorithm.SNAPPY);

                table.addFamily(hcd);
                admin.createTable(table);
            }
        }
    }

    public static void createTableWithSnappyCompress(String tableFullName) throws IOException {
        createTableWithSnappyCompress(tableFullName, "f1");
    }

    public static void put(String tableFullName, Put p, boolean createNotExistTable) throws IOException {
        batchPut(tableFullName, Collections.singletonList(p), createNotExistTable);
    }

    public static void batchPut(String tableFullName, List<Put> puts) throws IOException {
        batchPut(tableFullName, puts, false);
    }

    public static void batchPut(String tableFullName, List<Put> puts, boolean createNotExistTable) throws IOException {
        if (createNotExistTable)
            createTableWithSnappyCompress(tableFullName);

        try (Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(tableFullName))) {
            table.put(puts);
        }
    }

    public static void createRawDataTable() {
        String tableName = "log:raw_data";
        try {
            createTableWithSnappyCompress(tableName);
        } catch (IOException e) {
            logger.error("", e);
        }
        logger.info("created");
    }

    public static void createRawDataRequiredTable() {
        String tableName = "log:raw_data_required";
        try {
            createTableWithSnappyCompress(tableName);
        } catch (IOException e) {
            logger.error("", e);
        }
        logger.info("created");
    }

    public static void createAppLogTable() {
        String tableName = "log:app_log";
        try {
            createTableWithSnappyCompress(tableName);
        } catch (IOException e) {
            logger.error("", e);
        }
        logger.info("created");
    }

    public static void createLogCountTable() {
        String tableName = "realtime:log_count";
        try {
            createTableWithSnappyCompress(tableName, "cf");
        } catch (IOException e) {
            logger.error("", e);
        }
        logger.info("created");
    }


    public static void main(String[] args) throws IOException {
        createRawDataRequiredTable();
//        createTableWithSnappyCompress("chen:test");
//        TableName tn = TableName.valueOf("chenq");
//        try (Admin admin = HBaseConnectionFactory.getConnection().getAdmin()) {
//            if (!admin.tableExists(tn)) {
//                HTableDescriptor table = new HTableDescriptor(tn);
//                table.setCompactionEnabled(true);
//
//                {
//                    HColumnDescriptor hcd = new HColumnDescriptor("f1");
//                    hcd.setMaxVersions(1);
//                    hcd.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
//                    hcd.setCompressionType(Compression.Algorithm.SNAPPY);
//
//                    table.addFamily(hcd);
//                }
//                {
//                    HColumnDescriptor hcd = new HColumnDescriptor("f2");
//                    hcd.setMaxVersions(1);
//                    hcd.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
//                    hcd.setCompressionType(Compression.Algorithm.SNAPPY);
//
//                    table.addFamily(hcd);
//                }
//                admin.createTable(table);
//            }
//        }
        // List<NamespaceDescriptor> nds = listNamespace();
        // for (NamespaceDescriptor nd : nds) {
        // System.out.println(nd.getName());
        // }
        // createRawDataTable();
        // try (Admin admin = HBaseConnectionFactory.getConnection().getAdmin()) {
        // if (admin.tableExists(TableName.valueOf(tableName))) {
        // try (Table table = HBaseConnectionFactory.getConnection().getTable(TableName.valueOf(tableName))) {
        // byte[] startRowKey = "row1".getBytes();
        // byte[] stopRowKey = Bytes.unsignedCopyAndIncrement(startRowKey);
        //
        // Scan scan = new Scan(startRowKey, stopRowKey);
        // scan.setCaching(20000);
        // scan.setCacheBlocks(false);
        //
        // try (ResultScanner scanner = table.getScanner(scan)) {
        // for (Result rs : scanner) {
        // String rowKey = Bytes.toString(rs.getRow());
        //
        // byte[] c1ValueByte = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
        // String c1ValueStr = Bytes.toString(c1ValueByte);
        //
        // byte[] c2ValueByte = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c2"));
        // String c2ValueStr = Bytes.toString(c2ValueByte);
        //
        // byte[] c3ValueByte = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c3"));
        // String c3ValueStr = Bytes.toString(c3ValueByte);
        //
        // System.out.println(rowKey);
        // System.out.println(c1ValueStr);
        // System.out.println(c2ValueStr);
        // System.out.println(c3ValueStr);
        // }
        // }
        // }
        // }
        // }
    }
}