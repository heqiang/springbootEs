package com.example;

import com.alibaba.fastjson.JSON;
import com.example.pojo.User;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.SuggestingErrorOnUnknown;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class SpringbootEsApplicationTests {
    @Autowired
//    @Qualifier("restHighLevelClient")
    private RestHighLevelClient  restHighLevelClient;

    @Test
    void ceShi() throws IOException {
        // 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("hq_index1");
        // 客户端执行请求 执行创建请求 获得响应
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }
    // 添加文档
    @Test
    public  void  add_doc() throws IOException {
           IndexRequest  request = new IndexRequest("hq_index1");
           User  user  = new User(1,"何强2",16);
           request.id("1");
           request.timeout("1s");
           request.source(JSON.toJSONString(user), XContentType.JSON);
            IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);
            System.out.println(index.toString());
            System.out.println(index.status());

    }
    // 获取文档 判断是否存在
    @Test
    public  void  docExists() throws IOException {
        GetRequest getRequest = new GetRequest("hq_index1","1");
        // 不获取返回的_source的上下文
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
        //获取文档信息
    }
    // 获取文档
    @Test
    public  void  get_doc() throws IOException {
        GetRequest getRequest = new GetRequest("hq_index1", "1");
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
    }
    // 更新文档
    @Test
    public  void  update() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("hq_index1","1");
        User user = new User(2,"修改",11);
        updateRequest.doc(JSON.toJSONString(user),XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
    }

    //删除文档
    @Test
    public void  deleteDoc() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("hq_index","FsUaBHkBBRKa5NugsTEO");
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete);
    }
    @Test
    public void  testBulkAddRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<User>  users = new ArrayList<User>();
        users.add(new User(7,"hq",11));
        users.add(new User(2,"hq",11));
        users.add(new User(3,"hq",11));
        users.add(new User(4,"hq",11));
        users.add(new User(5,"hq",11));
        users.add(new User(6,"hq",11));
        for(int i=0;i<users.size();i++){
            bulkRequest.add(
                    new IndexRequest("hq_index1").id(""+(i+1)).source(JSON.toJSONString(users.get(i)),XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.hasFailures());
    }

    //查询
    @Test
    public  void  testSeach() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hq_index1");
        //构建搜索的条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.highlighter().preTags("</p class='key' style='color:red'>").postTags("</p>");

        //使用QueryBuilders工具类实现条件的构建
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name","hq");
        //进行查询
        searchSourceBuilder.query(termQueryBuilder);
        //对查询结果进行分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(3);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(search.getHits().getHits()));
    }

}
