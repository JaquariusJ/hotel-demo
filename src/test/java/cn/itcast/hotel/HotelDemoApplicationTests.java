package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
class HotelDemoApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private IHotelService hotelService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void createIndexTest() throws IOException {
        ClassPathResource classPathResource = new ClassPathResource("hotel.json");
        String s = FileUtils.readFileToString(classPathResource.getFile(),"utf8");
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        request.source(s, XContentType.JSON);
        restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void deleteIndexTest() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        restHighLevelClient.indices().delete(request,RequestOptions.DEFAULT);
    }

    @Test
    void existIndexTest() throws IOException {
        GetIndexRequest request = new GetIndexRequest("hotel");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }


    @Test
    void createDocumentTest() throws IOException {
        Hotel hotel = hotelService.getById(36934);
        HotelDoc hotelDoc = new HotelDoc(hotel);
        IndexRequest request = new IndexRequest("hotel").id("1");
        request.source(objectMapper.writeValueAsString(hotelDoc),XContentType.JSON);
        restHighLevelClient.index(request,RequestOptions.DEFAULT);
    }

    @Test
    void getDocumentTest() throws IOException {
        GetRequest request = new GetRequest("hotel","1");
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
    }

    @Test
    void updateDocumentTest() throws IOException {
        UpdateRequest request = new UpdateRequest("hotel","1");
        Map<String,Object> paramMap = new HashMap<>();
        paramMap.put("price",250);
        paramMap.put("starName","五钻");
        request.doc(paramMap);
        restHighLevelClient.update(request, RequestOptions.DEFAULT);
    }
    @Test
    void deleteDocumentTest() throws IOException {
        DeleteRequest request = new DeleteRequest("hotel","1");
        restHighLevelClient.delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void batchInsertDocumentTest() throws IOException {
        BulkRequest request = new BulkRequest();
        List<Hotel> hotels = hotelService.list();
        hotels.forEach(hotel -> {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            IndexRequest indexRequest = null;
            try {
                indexRequest = new IndexRequest("hotel").id(String.valueOf(hotelDoc.getId())).source(objectMapper.writeValueAsString(hotelDoc), XContentType.JSON);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            request.add(indexRequest);
        });
        restHighLevelClient.bulk(request,RequestOptions.DEFAULT);

    }




}
