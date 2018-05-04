package com.sf.es.core.senior;

import com.sf.es.core.common.EsClientUtils;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * Created by xuery on 2018/5/3.
 */
public class MultiQueryCarInfoApp {

    public static void main(String[] args) {
        TransportClient client = EsClientUtils.getTransportClient();

        try {
            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchQuery("brand", "宝马"))
                    .mustNot(QueryBuilders.termQuery("name.raw", "宝马318"))
                    .should(QueryBuilders.termQuery("produce_date", "2017-01-02"))
                    .filter(QueryBuilders.rangeQuery("price").gte("280000").lt("350000"));

            SearchResponse response = client.prepareSearch("car_shop")
                    .setTypes("cars")
                    .setQuery(queryBuilder)
                    .get();
            for (SearchHit searchHit : response.getHits().getHits()) {
                System.out.println(searchHit.getSourceAsString());
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
