package com.study.elasticsearch;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

/**
 * @author ChenYu ren
 * @date 2024/8/7
 */
public class Main {

    public static final String server_Url = "http://localhost:9200";

    public static final String API_KEY = "";


    private static ElasticsearchClient client;

    static {
        client = getClient();
    }


    public static void main(String[] args) throws Exception {
        IndexService indexService = new IndexService(client);
        DocumentService documentService = new DocumentService(client);
        String index = "user";
//        UserEntity one = new UserEntity();
//        one.setId(1);
//        one.setName("陈昱任");
//        one.setAge(11);
//        one.setSex("男");
//        UserEntity two = new UserEntity();
//        two.setId(2);
//        two.setName("小明");
//        two.setAge(22);
//        two.setSex("男");
//        UserEntity three = new UserEntity();
//        three.setId(3);
//        three.setName("小英");
//        three.setAge(33);
//        three.setSex("女");
//        UserEntity four = new UserEntity();
//        four.setId(4);
//        four.setName("小勒");
//        four.setAge(44);
//        four.setSex("女");
//        UserEntity five = new UserEntity();
//        five.setId(5);
//        five.setName("小码");
//        five.setAge(55);
//        five.setSex("女");
//        UserEntity six = new UserEntity();
//        six.setId(6);
//        six.setName("小测");
//        six.setAge(66);
//        six.setSex("男");
//        List<UserEntity> userList = Arrays.asList(one, two, three, four, five, six);
//        //1.批量新增数据
//        documentService.batchCreatDocument(index,userList);
//
//        //2.查询该索引下全部文档
//        documentService.searchAllDocumentByIndex(index);



//        documentService.batchCreatDocument(index, Arrays.asList(firstUser,secondUser,threeUser));

//
        //索引库操作
//        indexService.creatIndex(index);
//        indexService.getIndex(index);
//        indexService.delIndex(index);
        //文档操作
//        documentService.creatDocument(index,firstUser);
//
//        documentService.creatDocument(index,secondUser);
//
//        documentService.getDocument(index,secondUser.getId().toString());
//
//        documentService.updateDocument(index,secondUserUpdate);
//
//        documentService.getDocument(index,secondUser.getId().toString());
//
//        documentService.delDocument(index,secondUser.getId().toString());
//
//        documentService.searchDocument(index,"age","66");
//        documentService.searchDocumentPage(index,0,2);

//        documentService.searchDocSortByIndex(index,"age", SortOrder.Desc);
//        documentService.searchDocMultipleConditionsByIndex(index);
//        documentService.searchDocRangeByIndex(index,"age","11","44");

//        documentService.searchDocHighlightByIndex(index,"name","小");
//        documentService.searchDocMaxByIndex(index,"age");
        documentService.searchDocGroupByIndex(index,"sex");
    }
    public static void clearDoc()throws Exception{
        DocumentService documentService = new DocumentService(client);
        documentService.delDocument("user","1");
        documentService.delDocument("user","2");

    }














    /**
     * 获取客户端
     * @return client
     */
    public static ElasticsearchClient getClient() {
        // 创建客户端
        RestClient restClient = RestClient
                .builder(HttpHost.create(server_Url))
                .build();

        //使用Jackson映射器创建传输
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());
        //创建API客户端
        return new ElasticsearchClient(transport);
    }


}
