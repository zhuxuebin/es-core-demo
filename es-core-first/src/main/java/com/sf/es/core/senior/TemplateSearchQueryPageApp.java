package com.sf.es.core.senior;

import com.sf.es.core.common.EsClientUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xuery on 2018/5/3.
 */
public class TemplateSearchQueryPageApp {

    public static void main(String[] args) {
        templateSearch();
    }

    private static void templateSearch(){
        TransportClient client = EsClientUtils.getTransportClient();
        try{

            Map<String,Object> scriptParams = new HashMap<>();
            scriptParams.put("from",0);
            scriptParams.put("size",3);
            scriptParams.put("brand","宝马");

            SearchResponse searchResponse = new SearchTemplateRequestBuilder(client)
                    .setScript("page_query_by_brand")
                    .setScriptType(ScriptType.FILE)
                    .setScriptParams(scriptParams)
                    .setRequest(new SearchRequest("car_shop").types("sales"))
                    .get()
                    .getResponse();
            for(SearchHit searchHit : searchResponse.getHits().getHits()){
                System.out.println(searchHit.getSourceAsString());
            }
        } catch (Exception e){

        } finally {
            client.close();
        }
    }
}
