import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class PutListExample {
 public static void main(String[] args) throws IOException {
  Configuration conf = HBaseConfiguration.create();
  conf.set("hbase.zookeeper.quorum", "ip-20-0-31-210.ec2.internal");
  conf.set("hbase.zookeeper.property.clientPort", "2181");
  HTable table = new HTable(conf, "testtable_albtvenki");
  List < Put > puts = new ArrayList < Put > ();
  Put put1 = new Put(Bytes.toBytes("row1"));
  put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
   Bytes.toBytes("val1"));
  puts.add(put1);
  Put put2 = new Put(Bytes.toBytes("row2"));
  put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
   Bytes.toBytes("val2"));
  puts.add(put2);
  Put put3 = new Put(Bytes.toBytes("row2"));
  put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
   Bytes.toBytes("val3"));
  puts.add(put3);
  table.put(puts);
 }
}