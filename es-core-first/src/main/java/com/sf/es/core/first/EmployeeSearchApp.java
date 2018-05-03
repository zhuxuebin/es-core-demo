package com.sf.es.core.first;

import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;


/**
 * Created by xuery on 2018/4/8.
 */
public class EmployeeSearchApp {

    public static void main(String[] args) throws Exception {

        Settings settings = Settings.builder()
                .put("cluster.name", "my-es")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

//        prepareData(client);

        //查询
        searchData(client);
    }

    private static void prepareData(TransportClient client) throws IOException{
        client.prepareIndex("company", "index", "1")
                .setSource(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field("name", "jack")
                                .field("age", 27)
                                .field("position", "technique")
                                .field("join_date", "2017-01-01")
                                .field("salary", 10000)
                                .endObject()
                ).get();
    }

    private static void searchData(TransportClient client) throws IOException{
        SearchResponse searchResponse = client.prepareSearch("company")
                .setTypes("employee")
                .setQuery(QueryBuilders.termQuery("position","technique"))
                .setPostFilter(QueryBuilders.rangeQuery("age").from(30).to(40))
                .setFrom(0).setSize(1)
                .execute().actionGet();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] searchHitArr = searchHits.getHits();
        for (SearchHit searchHit: searchHitArr){
            System.out.println(searchHit.getSourceAsString());
        }
    }
}
