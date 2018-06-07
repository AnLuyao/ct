import com.utils.ConnectionInstance;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;

/**
 * @author AnLuyao
 * @date 2018-06-04 9:49
 */
public class HbaseScanTest {
   @Test
    public void scanData() throws ParseException, IOException {
//     14314302040 1月到4月所有通话数据
       String phone = "15133295266";
       String startPoint = "2019-01";
       String stopPoint = "2019-05";

//       Scan scan = new Scan();
       HbaseScanUtil scanUtil = new HbaseScanUtil();
       scanUtil.init(phone, startPoint, stopPoint);
       Connection connection = ConnectionInstance.getInstance();
       Table table = connection.getTable(TableName.valueOf("ct:calllog"));

       while (scanUtil.hasNext()) {
           String[] rowKeys = scanUtil.next();
           Scan scan = new Scan();
           scan.setStartRow(Bytes.toBytes(rowKeys[0]));
           scan.setStopRow(Bytes.toBytes(rowKeys[1]));

           System.out.println("时间范围："+rowKeys[0].split("_")[2]+"======="+rowKeys[1].split("_")[2]);

           ResultScanner scanner = table.getScanner(scan);
           for (Result result : scanner) {
               System.out.println(Bytes.toString(result.getRow()));
           }

       }


   }
}
