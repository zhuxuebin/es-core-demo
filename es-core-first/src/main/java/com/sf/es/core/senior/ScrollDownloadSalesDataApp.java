package com.sf.es.core.senior;

import com.sf.es.core.common.EsClientUtils;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * Created by xuery on 2018/5/3.
 */
public class ScrollDownloadSalesDataApp {

    public static void main(String[] args) {
        scrollSearchSalesData();
    }

    public static void scrollSearchSalesData(){
        TransportClient client = EsClientUtils.getTransportClient();
        try{
            SearchResponse scrollResp = client.prepareSearch("car_shop")
                    .setTypes("sales")
                    .setScroll(new TimeValue(60000))
                    .setQuery(QueryBuilders.termQuery("brand.keyword","宝马"))
                    .setSize(2)
                    .get();
            int count = 1;
            while(scrollResp.getHits().getHits().length != 0) {
                System.out.println("scroll round"+count+++"================");
                for (SearchHit searchHit : scrollResp.getHits().getHits()) {
                    System.out.println(searchHit.getSourceAsString());
                }
                //接上次的scrollId开始
                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                        .setScroll(new TimeValue(60000))
                        .execute()
                        .actionGet();
            }
        } catch (Exception e){

        } finally {
            client.close();
        }
    }
}
