package com.study.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import lombok.AllArgsConstructor;

import java.io.IOException;

/**
 * @author ChenYu ren
 * @date 2024/8/9
 */

@AllArgsConstructor
public class IndexService {

    private ElasticsearchClient client;


    /**
     * 创建索引库
     * @param index 索引
     * @throws IOException 异常
     */
    public void creatIndex(String index) throws IOException {
        //创建索引请求
        CreateIndexResponse createIndexResp = client.indices().create(builder -> builder.index(index));
        boolean acknowledged = createIndexResp.acknowledged();
        System.out.printf("索引{%s} 创建结果 ->%s%n",index,acknowledged);
    }


    /**
     * 查询索引
     * @param index 索引名称
     * @throws Exception 异常
     */
    public void getIndex(String index)throws Exception {
        GetIndexResponse getIndexResponse = client.indices().get(builder -> builder.index(index));
        System.out.printf("索引(%s)查询结果:settings ->%s%n",index, getIndexResponse.get(index).settings().toString());
        System.out.printf("索引(%s)查询结果:mappings ->%s%n",index,  getIndexResponse.get(index).mappings().toString());
        System.out.printf("索引(%s)查询结果:aliases ->%s%n",index,  getIndexResponse.get(index).aliases().toString());
    }


    /**
     * 删除索引
     * @param index 索引名称
     * @throws Exception 异常
     */
    public void delIndex(String index) throws Exception{
        DeleteIndexResponse deleteIndexResponse = client.indices().delete(d -> d.index(index));
        System.out.printf("删除索引 {%s} 结果 -> %s%n",index, deleteIndexResponse.acknowledged());
    }




}
