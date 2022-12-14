package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Map;

public class HotelSearchTest {

    private RestHighLevelClient client;

    @Test
    void  testAgg() throws IOException {
        SearchRequest request = new SearchRequest("hotel");

        request.source().size(0);

        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand"))
                .size(10);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        Aggregations aggregations = response.getAggregations();
        Terms brandTerms = aggregations.get("brandAgg");
        brandTerms.getBuckets().forEach(bucket -> {
            System.out.println(bucket.getKey() + ":" + bucket.getDocCount());
        });
    }


    @Test
    void testMatchAll() throws IOException {
        // 1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.??????DSL
        request.source()
                .query(QueryBuilders.matchAllQuery());
        // 3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 4.????????????
        handleResponse(response);
    }

    @Test
    void testMatch() throws IOException {
        // 1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.??????DSL
        request.source()
                .query(QueryBuilders.matchQuery(
                        "all",
                        "??????"
                ));
        // 3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 4.????????????
        handleResponse(response);
    }

    @Test
    void testBool() throws IOException {
        // 1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.??????DSL
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        boolQueryBuilder.must(QueryBuilders.termQuery("city","??????"));
        boolQueryBuilder.mustNot(QueryBuilders.rangeQuery("price").lt(300));
        boolQueryBuilder.should(
                QueryBuilders.termQuery("name","??????")
        );

        request.source().query(boolQueryBuilder);
        // 3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 4.????????????
        handleResponse(response);
    }

    @Test
    void testPageAndSort() throws IOException {
        // ?????????????????????
        int page = 1, size = 5;

        // 1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.??????DSL
        // 2.1.query
        request.source().query(QueryBuilders.matchAllQuery());
        // 2.2.?????? sort
        request.source().sort("price", SortOrder.ASC);
        // 2.3.?????? from???size
        request.source().from((page - 1) * size).size(5);
        // 3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.????????????
        handleResponse(response);

    }

    @Test
    void testHighlight() throws IOException {
        // 1.??????Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.??????DSL
        // 2.1.query
        request.source().query(QueryBuilders.matchQuery("all", "??????"));
        // 2.2.??????
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        // 3.????????????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.????????????
        handleResponse(response);

    }






    private void handleResponse(SearchResponse response) {
        // 4.????????????
        SearchHits searchHits = response.getHits();
        // 4.1.???????????????
        long total = searchHits.getTotalHits().value;
        System.out.println("????????????" + total + "?????????");
        // 4.2.????????????
        SearchHit[] hits = searchHits.getHits();
        // 4.3.??????
        for (SearchHit hit : hits) {
            // ????????????source
            String json = hit.getSourceAsString();
            // ????????????
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            // ??????????????????
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                // ?????????????????????????????????
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null) {
                    // ???????????????
                    String name = highlightField.getFragments()[0].string();
                    // ?????????????????????
                    hotelDoc.setName(name);
                }
            }
            System.out.println("hotelDoc = " + hotelDoc);
        }
    }







    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(
                // ??????????????????
                RestClient.builder(
                        HttpHost.create("localhost:9200")
//                        HttpHost.create("localhost:9201"),
//                        HttpHost.create("localhost:9202")
                )
        );
    }

    @SneakyThrows
    @AfterEach
    void tearDown() {
        this.client.close();
    }
}
