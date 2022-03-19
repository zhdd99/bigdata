package org.example.homework02;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author zhangdongdong <zhangdongdong@kuaishou.com>
 * Created on 2022-03-19
 */
public class BaseTest {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        if (!Arrays.stream(admin.listNamespaceDescriptors())
                .anyMatch(namespaceDescriptor -> "zhangdongdong".equals(namespaceDescriptor.getName()))) {
            admin.createNamespace(NamespaceDescriptor.create("zhangdongdong").build());
            System.out.println("创建namspace: zhangdongdong, 成功！");
        }

        TableName tableName = TableName.valueOf("zhangdongdong:student");
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor("info"));
            hTableDescriptor.addFamily(new HColumnDescriptor("score"));
            admin.createTable(hTableDescriptor);
            System.out.println("Table Create successful");
        }

        // 插入数据 Tom, Jerry, Jack, Rose, zhangdongdong
        Put put = new Put(Bytes.toBytes("Tom")); // row key
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000001"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("75"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("82"));
        conn.getTable(tableName).put(put);

        put = new Put(Bytes.toBytes("Jerry")); // row key
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000002"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("85"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("67"));
        conn.getTable(tableName).put(put);

        put = new Put(Bytes.toBytes("Jack")); // row key
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000003"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("80"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("80"));
        conn.getTable(tableName).put(put);

        put = new Put(Bytes.toBytes("Rose")); // row key
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000004"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("60"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("61"));
        conn.getTable(tableName).put(put);

        put = new Put(Bytes.toBytes("zhangdongdong")); // row key
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("G20210579020459"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("5"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("100"));
        put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("100"));
        conn.getTable(tableName).put(put);

        System.out.println("Data insert success");

        // 查看数据
        Scan scan = new Scan();
        ResultScanner scanner = conn.getTable(tableName).getScanner(scan);
        scanner.iterator().forEachRemaining(result -> {
            List<Cell> cells = result.listCells();
            byte[] row = result.getRow();
            String rowKey = Bytes.toString(row);

            System.out.println("rowKey为"+rowKey);
            Map<String, String> rowValue = new HashMap<>();
            for (Cell cell : cells) {
                rowValue.put(Bytes.toString(CellUtil.cloneFamily(cell))+ ":" + Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println(rowValue);
            System.out.println("************************");
        });

        // 删除数据
        conn.getTable(tableName).delete(new Delete(Bytes.toBytes("Tom")));
        conn.getTable(tableName).delete(new Delete(Bytes.toBytes("Jerry")));
        conn.getTable(tableName).delete(new Delete(Bytes.toBytes("Jack")));
        conn.getTable(tableName).delete(new Delete(Bytes.toBytes("Rose")));
        conn.getTable(tableName).delete(new Delete(Bytes.toBytes("zhangdongdong")));
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }
}
