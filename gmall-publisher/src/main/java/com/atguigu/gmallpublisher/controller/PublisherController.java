package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {
        //1.创建list集合用来存放最终结果数据
        ArrayList<Map> result = new ArrayList<>();

        //2.创建存放新增日活的Map
        //获取日活总数数据
        Integer dauTotal = publisherService.getDauTotal(date);

        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //3.创建存放新增设备的Map
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        result.add(dauMap);
        result.add(devMap);


        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauHourTotal(@RequestParam("id") String id,
                                  @RequestParam("date") String date) {

        //1.获取昨天的日期
        String yesterdayDate = LocalDate.parse(date).plusDays(-1).toString();

        //2.获取今天的分时数据
        Map todayMap = publisherService.getDauHourTotal(date);

        //3.获取昨天的分时数据
        Map yesterdayMap = publisherService.getDauHourTotal(yesterdayDate);

        //4.创建存放结果数据的Map集合
        HashMap<String, Map> result = new HashMap<>();

        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSONObject.toJSONString(result);
    }
}
