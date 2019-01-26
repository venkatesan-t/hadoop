import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
public class RowFilterExample {
 public static void main(String[] args) throws IOException {
  Configuration conf = HBaseConfiguration.create();
  conf.set("hbase.zookeeper.quorum", "ip-20-0-31-210.ec2.internal");
  conf.set("hbase.zookeeper.property.clientPort", "2181");
  System.out.println(conf.get("hbase.zookeeper.quorum"));
  System.out.println(conf.get("hbase.zookeeper.property.clientPort"));
  HTable table = new HTable(conf, "customers_albtvenki");
  Scan scan = new Scan();
  scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fname"));
  Filter filter1 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
   new BinaryComparator(Bytes.toBytes("4000010")));
  scan.setFilter(filter1);
  ResultScanner scanner1 = table.getScanner(scan);
  System.out.println("Scanning table #1...");
  for (Result res: scanner1) {
   System.out.println(res);
  }
  scanner1.close();
  Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,
   new RegexStringComparator("40000 ?(09|12|15)"));
  scan.setFilter(filter2);
  ResultScanner scanner2 = table.getScanner(scan);
  System.out.println("Scanning table #2...");
  for (Result res: scanner2) {
   System.out.println(res);
  }
  scanner2.close();
  Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL,
   new SubstringComparator("5555"));
  scan.setFilter(filter3);
  ResultScanner scanner3 = table.getScanner(scan);
  System.out.println("Scanning table #3...");
  for (Result res: scanner3) {
   System.out.println(res);
  }
  scanner3.close();
 }
}