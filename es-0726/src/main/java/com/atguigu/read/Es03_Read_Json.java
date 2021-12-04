package com.atguigu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Es03_Read_Json {
    public static void main(String[] args) throws IOException {
        //1.获取客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.读取ES中的数据，并解析
        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"男\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"favo\": \"乒乓球\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupByClass\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"class_id\"\n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"groupByAge\": {\n" +
                "          \"max\": {\n" +
                "            \"field\": \"age\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 0,\n" +
                "  \"size\": 2\n" +
                "}")
                .addIndex("student")
                .addType("_doc")
                .build();
        SearchResult result = jestClient.execute(search);

        //TODO 获取命中条数
        System.out.println("命中条数："+result.getTotal());

        //TODO 获取明细数据
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            //获取索引名
            System.out.println("_index:"+hit.index);

            //获取类型名
            System.out.println("_type:"+hit.type);

            //获取docid
            System.out.println("_id:"+hit.id);

            Map source = hit.source;

            //获取明细数据
            Set keySet = source.keySet();
            for (Object o : keySet) {
                System.out.println(o.toString()+":"+source.get(o));
            }
        }

        //TODO 获取聚合组数据
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key:"+bucket.getKey());
            System.out.println("doc_count:"+bucket.getCount());
            //获取嵌套的聚合组数据（按照年龄聚合）
            MaxAggregation groupByAge = bucket.getMaxAggregation("groupByAge");
            System.out.println("value:"+groupByAge.getMax());
        }


        jestClient.shutdownClient();
    }
}
