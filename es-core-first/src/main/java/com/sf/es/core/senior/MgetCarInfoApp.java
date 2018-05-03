package com.sf.es.core.senior;

import com.sf.es.core.common.EsClientUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.transport.TransportClient;

/**
 * Created by xuery on 2018/5/2.
 */
public class MgetCarInfoApp {

    public static void main(String[] args) {
        mgetCarInfo();
    }

    public static void mgetCarInfo(){
        TransportClient client = EsClientUtils.getTransportClient();
        try {
            if (client != null) {
                MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                        .add("car_shop", "cars", "1")
                        .add("car_shop", "cars", "2")
                        .get();
                for (MultiGetItemResponse multiGetItemResponse : multiGetItemResponses) {
                    GetResponse getResponse = multiGetItemResponse.getResponse();
                    if (getResponse.isExists()) {
                        System.out.println(getResponse.getSourceAsString());
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
