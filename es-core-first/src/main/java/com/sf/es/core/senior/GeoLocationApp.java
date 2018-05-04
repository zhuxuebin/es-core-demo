package com.sf.es.core.senior;

import com.sf.es.core.common.EsClientUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xuery on 2018/5/3.
 */
public class GeoLocationApp {

    public static void main(String[] args) {
        twoPointArea();
    }

    private static void twoPointArea() {
        TransportClient client = EsClientUtils.getTransportClient();

        //1. 搜索两个坐标点组成的一个区域
        QueryBuilder qb = QueryBuilders.geoBoundingBoxQuery("pin.location").setCorners(40.73, -74.1, 40.01, -71.12);

        //2. 指定一个区域，由三个坐标点，组成，比如上海大厦，东方明珠塔，上海火车站
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(40.73, -74.1));
        points.add(new GeoPoint(40.01, -71.12));
        points.add(new GeoPoint(50.56, -90.58));
        QueryBuilder qb1 = QueryBuilders.geoPolygonQuery("pin.location", points);

        //3. 查询距离某个地点，方圆几公里的酒店
        QueryBuilder qb2 = QueryBuilders.geoDistanceQuery("pin.location").point(40,-70).distance(200, DistanceUnit.KILOMETERS);

        SearchResponse response = client.prepareSearch("car_shop")
                .setTypes("shops")
                .setQuery(qb)
                .get();

        for (SearchHit searchHit : response.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }
    }
}
