package com.sf.es.core.senior;

import com.sf.es.core.common.EsClientUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * Created by xuery on 2018/5/3.
 */
public class VariousQuerySalesDataApp {

    public static void main(String[] args) {
        TransportClient client = EsClientUtils.getTransportClient();

        SearchResponse response = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.matchQuery("brand","宝马"))
                .get();

        System.out.println("=========matchQuery==============");
        for(SearchHit searchHit : response.getHits().getHits()){
            System.out.println(searchHit.getSourceAsString());
        }

        System.out.println("=========multiMatchQuery==============");
        SearchResponse response1 = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.multiMatchQuery("宝马","brand","name"))
                .get();

        for(SearchHit searchHit : response1.getHits().getHits()){
            System.out.println(searchHit.getSourceAsString());
        }

        System.out.println("=========commonMatchQuery==============");
        SearchResponse response2 = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.commonTermsQuery("name","宝马320"))
                .get();
        for(SearchHit searchHit : response2.getHits().getHits()){
            System.out.println(searchHit.getSourceAsString());
        }

        System.out.println("=========prefixQuery==============");
        //发现当name=x宝马时也能查出来，这是因为name会分词，改成name.keyword应该就查不出来了,也不行，啥都出不出来了，prefix不适用于name.keyword
        SearchResponse response3 = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.prefixQuery("name","宝"))
                .get();
//        SearchResponse response3 = client.prepareSearch("car_shop")
//                .setTypes("cars")
//                .setQuery(QueryBuilders.prefixQuery("name.keyword","宝"))
//                .get();
        for(SearchHit searchHit : response3.getHits().getHits()){
            System.out.println(searchHit.getSourceAsString());
        }
    }
}
