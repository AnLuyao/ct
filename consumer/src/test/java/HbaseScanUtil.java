import com.utils.HbaseUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author AnLuyao
 * @date 2018-06-04 10:05
 */
public class HbaseScanUtil {
    private List<String[]> list;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
    private int i = 0;

    public void init(String phone, String start, String stop) throws ParseException {
        list = new ArrayList<>();
        Date startDate = sdf.parse(start);
        Date stopDate = sdf.parse(stop);
//      当前开始时间
        Calendar startPoint = Calendar.getInstance();
        startPoint.setTime(startDate);
//      当前结束时间
        Calendar stopPoint = Calendar.getInstance();
        stopPoint.setTime(startDate);
        stopPoint.add(Calendar.MONTH, 1);
        while (stopPoint.getTimeInMillis() <= stopDate.getTime()) {
//            long millis = startPoint.getTimeInMillis();
            String startTime = sdf.format(startPoint.getTime());
            String stopTime = sdf.format(stopPoint.getTime());

            String regionHash = HbaseUtil.getRegionHash(phone, startTime, 6);

            String startRow = regionHash + "_" + phone + "_" + startTime;
            String stopRow = regionHash + "_" + phone + "_" + stopTime;

            String[] rowKeys = {startRow, stopRow};
            list.add(rowKeys);
            startPoint.add(Calendar.MONTH, 1);
            stopPoint.add(Calendar.MONTH, 1);
        }
    }

    public boolean hasNext() {
        return i < list.size();
    }

    public String[] next() {
        return list.get(i++);
    }
}
