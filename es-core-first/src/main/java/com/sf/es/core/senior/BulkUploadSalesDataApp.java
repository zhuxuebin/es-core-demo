package com.sf.es.core.senior;

import com.sf.es.core.common.EsClientUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * Created by xuery on 2018/5/2.
 */
public class BulkUploadSalesDataApp {

    public static void main(String[] args) {
        bulkUploadSalesData();
    }

    public static void bulkUploadSalesData(){
        TransportClient client = EsClientUtils.getTransportClient();
        try{
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

            //create
            bulkRequestBuilder.add(client.prepareIndex("car_shop","sales","3")
                .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("brand", "奔驰")
                        .field("name", "奔驰C200")
                        .field("price", 350000)
                        .field("produce_date", "2017-01-05")
                        .field("sale_price", 340000)
                        .field("sale_date", "2017-02-03")
                        .endObject()
                )
            );
            //update
            bulkRequestBuilder.add(client.prepareUpdate("car_shop","sales","1")
                .setDoc(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("price",380000)
                        .endObject()
                )
            );
            //delete
            bulkRequestBuilder.add(client.prepareDelete("car_shop","sales","2"));

            BulkResponse bulkItemResponses = bulkRequestBuilder.get();
            for(BulkItemResponse bulkItemResponse : bulkItemResponses){
                System.out.println("version:"+bulkItemResponse.getVersion());
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
