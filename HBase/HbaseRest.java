import java.io.IOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;
public class HbaseRest {
 public static void main(String[] args) throws IOException {
  Cluster cluster = new Cluster();
  cluster.add("ip-20-0-21-196.ec2.internal", 20550);
  Client client = new Client(cluster);
  RemoteHTable table = new RemoteHTable(client, "employee");
  Get get = new Get(Bytes.toBytes("emp1"));
  get.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("ename"));
  Result result1 = table.get(get);
  System.out.println("Get result1: " + result1);
  Scan scan = new Scan();
  scan.setStartRow(Bytes.toBytes("emp1"));
  scan.setStopRow(Bytes.toBytes("emp5"));
  scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("ename"));
  ResultScanner scanner = table.getScanner(scan);
  for (Result result2: scanner) {
   System.out.println("Scan row[" + Bytes.toString(result2.getRow()) +
    "]: " + result2);
  }
 }
}