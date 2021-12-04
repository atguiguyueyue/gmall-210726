package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class Es02_BulkWrite {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取客户端连接
        JestClient jestClient = jestClientFactory.getObject();

        Movie movie104 = new Movie("104", "007:无暇赴死");
        Movie movie105 = new Movie("105", "007:大破杀机");
        Movie movie106 = new Movie("106", "007:量子危机");
        Movie movie107 = new Movie("107", "007:皇家赌场");

        Index index104 = new Index.Builder(movie104).id("1004").build();
        Index index105 = new Index.Builder(movie105).id("1005").build();
        Index index106 = new Index.Builder(movie106).id("1006").build();
        Index index107 = new Index.Builder(movie107).id("1007").build();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie_test1")
                .defaultType("_doc")
                .addAction(index104)
                .addAction(index105)
                .addAction(index106)
                .addAction(index107)
                .build();

        //4.批量写入数据
        jestClient.execute(bulk);


        //关闭连接
        jestClient.shutdownClient();
    }
}
