package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //获取日活总数数据
    public Integer selectDauTotal(String date);

    //获取日活分时数据
    public List<Map> selectDauTotalHourMap(String date);
}
