package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.RequestParam;
import cn.itcast.hotel.pojo.ResponseHotel;
import cn.itcast.hotel.service.IHotelService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.SQLOutput;
import java.util.*;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;


    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public ResponseHotel search(RequestParam requestParam){
        ResponseHotel responseHotel = null;
        try {
            SearchRequest request = new SearchRequest("hotel");
            FunctionScoreQueryBuilder functionSocreQuery = buildParam(requestParam,request);
            request.source().query(functionSocreQuery);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            responseHotel = response2Entity(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return responseHotel;
    }

    @Override
    public Map<String, List<String>> filters(RequestParam requestParams) {
        ResponseHotel responseHotel = null;
        HashMap<String, List<String>> resultMap = new HashMap<>();
        try {
            SearchRequest request = new SearchRequest("hotel");
            //构建请求
            FunctionScoreQueryBuilder functionSocreQuery = buildParam(requestParams,request);
            request.source().query(functionSocreQuery);
            request.source().size(0);
            //添加聚合
            buildAggration(request);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //获取返回结果
            List<String> brandAGG = getAggResult(response, "brandAgg");
            resultMap.put("brand",brandAGG);
            List<String> cityAgg = getAggResult(response, "cityAgg");
            resultMap.put("city",cityAgg);
            List<String> starAgg = getAggResult(response, "starAgg");
            resultMap.put("starName",starAgg);


        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }

    @Override
    public List<String> suggestion(String key) {
        List<String> results = new ArrayList<>();
        try {
            SearchRequest request = new SearchRequest("hotel");
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "keySuggest", SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(key)
                            .skipDuplicates(true)
                            .size(20)
            ));
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            Suggest suggest = response.getSuggest();
            CompletionSuggestion completionSuggestion = suggest.getSuggestion("keySuggest");
            List<CompletionSuggestion.Entry.Option> options = completionSuggestion.getOptions();
            for (CompletionSuggestion.Entry.Option option : options) {
                results.add(option.getText().toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    private void buildAggration(SearchRequest request) {
        request.source().aggregation(AggregationBuilders.terms("brandAgg").field("brand").size(100));
        request.source().aggregation(AggregationBuilders.terms("cityAgg").field("city").size(100));
        request.source().aggregation(AggregationBuilders.terms("starAgg").field("starName").size(100));
    }

    private List<String> getAggResult(SearchResponse response,String aggName) {
        List<String> list = new ArrayList<>();
        Aggregations aggregations = response.getAggregations();
        Terms terms = aggregations.get(aggName);
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String brandName = bucket.getKeyAsString();
            list.add(brandName);
        }
        return list;
    }

    private FunctionScoreQueryBuilder buildParam(RequestParam requestParam,SearchRequest request) {
        String key = requestParam.getKey();
        int page = requestParam.getPage() <= 0 ? 1 : requestParam.getPage();
        int size = requestParam.getSize() <= 0 ? 10 : requestParam.getSize();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if(StringUtils.isEmpty(key)){
            boolQuery.must(QueryBuilders.matchAllQuery());
        }else{
            boolQuery.must(QueryBuilders.matchQuery("all",key));
        }
        if(StringUtils.isNotEmpty(requestParam.getCity())){
            boolQuery.filter(QueryBuilders.termQuery("city",requestParam.getCity()));
        }
        if(StringUtils.isNotEmpty(requestParam.getBrand())){
            boolQuery.filter(QueryBuilders.termQuery("brand",requestParam.getBrand()));
        }
        if(StringUtils.isNotEmpty(requestParam.getStarName())){
            boolQuery.filter(QueryBuilders.termQuery("starName",requestParam.getStarName()));
        }
        if(requestParam.getMinPrice() !=null){
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(requestParam.getMinPrice()));
        }
        if(requestParam.getMaxPrice() !=null){
            boolQuery.filter(QueryBuilders.rangeQuery("price").lte(requestParam.getMaxPrice()));
        }
        // 算分控制
        FunctionScoreQueryBuilder functionSocreQuery = QueryBuilders.functionScoreQuery(
                boolQuery, // 原始查询，相关性算分的查询
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        // 其中的一个function score 元素
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                // 过滤条件
                                QueryBuilders.termQuery("isAD", true),
                                // 算分函数
                                ScoreFunctionBuilders.weightFactorFunction(5)
                        )
                }
        );
        //设置分页
        request.source().from((page-1)*size).size(size);
        //设置排序
        if(StringUtils.isNotEmpty(requestParam.getLocation())){
            request.source()
                    .sort(
                            SortBuilders.geoDistanceSort("location",new GeoPoint(requestParam.getLocation()))
                                    .order(SortOrder.ASC)
                                    .unit(DistanceUnit.KILOMETERS)

                    );
        }
        return functionSocreQuery;
    }

    private FunctionScoreQueryBuilder buildBasicQuery(RequestParam params, SearchRequest request) {
        // 1.构建BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 关键字搜索
        String key = params.getKey();
        if (key == null || "".equals(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        // 城市条件
        if (params.getCity() != null && !params.getCity().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }
        // 品牌条件
        if (params.getBrand() != null && !params.getBrand().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        // 星级条件
        if (params.getStarName() != null && !params.getStarName().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        // 价格
        if (params.getMinPrice() != null && params.getMaxPrice() != null) {
            boolQuery.filter(QueryBuilders
                    .rangeQuery("price")
                    .gte(params.getMinPrice())
                    .lte(params.getMaxPrice())
            );
        }

        // 2.算分控制
        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        // 原始查询，相关性算分的查询
                        boolQuery,
                        // function score的数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                // 其中的一个function score 元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        // 过滤条件
                                        QueryBuilders.termQuery("isAD", true),
                                        // 算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });
            return functionScoreQuery;
    }

    public ResponseHotel response2Entity(SearchResponse response){
        ResponseHotel responseEntity = new ResponseHotel();

        List<HotelDoc> list = new ArrayList<>();
        //获取结果
        SearchHit[] hits = response.getHits().getHits();
        TotalHits totalHits = response.getHits().getTotalHits();
        responseEntity.setTotal(totalHits.value);
        Arrays.stream(hits).forEach(hit -> {
            try {
                String hotelstr = hit.getSourceAsString();
                Hotel hotel = objectMapper.readValue(hotelstr, Hotel.class);
                HotelDoc hotelDoc = new HotelDoc(hotel);
                //设置高亮

                Object[] sortValues = hit.getSortValues();
                if(ArrayUtils.isNotEmpty(sortValues)){
                   Object sortValue = sortValues[0];
                   hotelDoc.setDistance(sortValue);
                }
                list.add(hotelDoc);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
        responseEntity.setHotels(list);
        return responseEntity;
    }
}
