package com.sf.es.core.senior;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * Created by xuery on 2018/5/2.
 */
public class UpsertCarInfoApp {

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "my-es")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        upsertCarInfo(client);

    }

    public static void upsertCarInfo(TransportClient client) throws Exception{

        //index -->source
        IndexRequest indexRequest = new IndexRequest("car_shop", "cars", "1")
                .source(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                    .field("brand", "宝马")
                                    .field("name", "宝马320")
                                    .field("price", 310000)
                                    .field("produce_date", "2017-01-01")
                                .endObject()
                );

        //update--post-->doc
        UpdateRequest updateRequest = new UpdateRequest("car_shop", "cars", "1")
                .doc(
                        XContentFactory.jsonBuilder()
                        .startObject()
                         .field("price", 300000)
                        .endObject()
                ).upsert(indexRequest);

        client.update(updateRequest).get();

    }
}
