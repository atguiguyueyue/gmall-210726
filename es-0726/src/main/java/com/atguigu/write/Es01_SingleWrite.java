package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class Es01_SingleWrite {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取客户端连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.将数据写入ES

        Movie movie = new Movie("103", "蝙蝠侠");

//        Index index = new Index.Builder("{\n" +
//                "  \"id\":\"102\",\n" +
//                "  \"name\":\"夏洛特烦恼\"\n" +
//                "}")
//                .index("movie_test1")
//                .type("_doc")
//                .id("1002")
//                .build();
        Index index = new Index.Builder(movie)
                .index("movie_test1")
                .type("_doc")
                .id("1003")
                .build();
        jestClient.execute(index);


        jestClient.shutdownClient();
    }
}
