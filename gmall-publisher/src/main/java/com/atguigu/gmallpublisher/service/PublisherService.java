package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //日活总数抽象方法
    public Integer getDauTotal(String date);

    //日活分时数据抽象方法
    public Map getDauHourTotal(String date);

    //交易额总数抽象方法
    public Double getGmvTotal(String date);

    //交易额分时数据抽象方法
    public Map getGmvHourTotal(String date);
}
