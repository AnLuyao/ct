package com;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 * @author AnLuyao
 * @date 2018-06-01 9:36
 * 生产数据
 */

public class Producer {
        ArrayList<String> phoneList = new ArrayList<>();
        HashMap<String, String> contacts  = new HashMap<>();
    private String start = "2019-01-01";
    private String end = "2020-01-01";
    public void init(){

        phoneList.add("15369468720");
        phoneList.add("19920860202");
        phoneList.add("18411925860");
        phoneList.add("14473548449");
        phoneList.add("18749966182");
        phoneList.add("19379884788");
        phoneList.add("19335715448");
        phoneList.add("18503558939");
        phoneList.add("13407209608");
        phoneList.add("15596505995");
        phoneList.add("17519874292");
        phoneList.add("15178485516");
        phoneList.add("19877232369");
        phoneList.add("18706287692");
        phoneList.add("18944239644");
        phoneList.add("17325302007");
        phoneList.add("18839074540");
        phoneList.add("19879419704");
        phoneList.add("16480981069");
        phoneList.add("18674257265");
        phoneList.add("18302820904");
        phoneList.add("15133295266");
        phoneList.add("17868457605");
        phoneList.add("15490732767");
        phoneList.add("15064972307");

        contacts.put("15369468720", "李雁");
        contacts.put("19920860202", "卫艺");
        contacts.put("18411925860", "仰莉");
        contacts.put("14473548449", "陶欣悦");
        contacts.put("18749966182", "施梅梅");
        contacts.put("19379884788", "金虹霖");
        contacts.put("19335715448", "魏明艳");
        contacts.put("18503558939", "华贞");
        contacts.put("13407209608", "华啟倩");
        contacts.put("15596505995", "仲采绿");
        contacts.put("17519874292", "卫丹");
        contacts.put("15178485516", "戚丽红");
        contacts.put("19877232369", "何翠柔");
        contacts.put("18706287692", "钱溶艳");
        contacts.put("18944239644", "钱琳");
        contacts.put("17325302007", "缪静欣");
        contacts.put("18839074540", "焦秋菊");
        contacts.put("19879419704", "吕访琴");
        contacts.put("16480981069", "沈丹");
        contacts.put("18674257265", "褚美丽");
    }
    private String productLog() throws ParseException {
        String callee;
        String caller;
        String buildTime;
        int  dura;
        int callerIndex = (int)(Math.random()*phoneList.size());
        caller = phoneList.get(callerIndex);
        while (true) {
            int calleeIndex = (int) (Math.random() * phoneList.size());
            callee = phoneList.get(calleeIndex);
            if (calleeIndex != callerIndex) {
                break;
            }
        }

            //2.随机生成通话建立时间
            buildTime = randomBuildTime(start,end);

            //3.随机生成通话时长
            dura = (int) (Math.random() * 30 * 60) + 1;
            String duration = new DecimalFormat("0000").format(dura);

        return  caller + "," + callee + "," + buildTime + "," + duration + "\n";
    }

    /**
     * 随机生成通话建立时间
     * @param start
     * @param end
     * @return
     */
    private String randomBuildTime(String start,String end) throws ParseException {
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startPoint = sdf1.parse(start).getTime();
        long endPoint = sdf1.parse(end).getTime();

        long resultTS = (long) (startPoint + Math.random() * (endPoint - startPoint));
        return sdf2.format(new Date(resultTS));
    }

    public void writeLog(String path) throws IOException, ParseException, InterruptedException {
        FileOutputStream fos = new FileOutputStream(path);
        OutputStreamWriter osw = new OutputStreamWriter(fos);
        while (true) {
            String log = productLog();
            System.out.println(log);
            osw.write(log);
            osw.flush();
            Thread.sleep(300);
        }

    }

    public static void main(String[] args) throws ParseException, InterruptedException, IOException {
        if (args.length <= 0) {
            System.out.println("没有参数");
            System.exit(0);
        }
        Producer producer = new Producer();
        producer.init();
        producer.writeLog(args[0]);

    }


}
