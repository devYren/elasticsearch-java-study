package com.study.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author ChenYu ren
 * @date 2024/8/9
 */

@AllArgsConstructor
public class DocumentService {

    private ElasticsearchClient client;

    /**
     * 创建 document
     * @param index 索引库
     * @param user 用户数据
     * @throws Exception ex
     */
    public void creatDocument(String index,UserEntity user) throws Exception {
        IndexResponse user1DocResp = client.index(builder ->
                //指定index -> user
                builder.index(index)
                        //唯一标识Id
                        .id(user.getId().toString())
                        //数据
                        .document(user));
        System.out.printf("向索引库(%s) 插入文档(%s) 返回结果 result -> %s%n",index,user.toString(),user1DocResp.result().jsonValue());
        System.out.printf("向索引库(%s) 插入文档(%s) 返回结果 Index -> %s%n",index, user,user1DocResp.index());
        System.out.printf("向索引库(%s) 插入文档(%s) 返回结果 ID -> %s%n",index, user,user1DocResp.id());
        System.out.printf("向索引库(%s) 插入文档(%s) 返回结果 Version -> %s%n",index, user,user1DocResp.version());
    }

    /**
     * 注意事项:在进行批量操作时,注意单个批量请求的大小,建议单个批量请求的大小不要超过 5-15MB。以避免影响性能。
     * @param index 索引库
     * @param users 多个用户数据
     * @throws Exception 异常
     */
    public void batchCreatDocument(String index, List<UserEntity> users) throws Exception{
        BulkRequest.Builder bulkRequest = new BulkRequest.Builder();
        users.forEach(user-> bulkRequest.operations(op -> op
                    .index(idx -> idx
                        .index(index)
                        .id(user.getId().toString())
                        .document(user))));
        BulkResponse bulkResponse = client.bulk(bulkRequest.build());
        // 检查批量操作的结果
        if (bulkResponse.errors()) {
            bulkResponse.items().forEach(item ->{
                System.out.printf("向索引库(%s) 插入文档【%s】失败,消耗时间:%s,errorMsg:【%s】%n",
                        index,
                        users,
                        bulkResponse.took(),
                        Objects.nonNull(item.error())
                        ? item.error().toString()
                        : "");
            });
        } else {
            System.out.printf("向索引库(%s) 插入文档【%s】成功,消耗时间:%s,items:【%s】%n",index,
                    users,
                    bulkResponse.took(),
                    bulkResponse.items());
        }
    }

    /**
     * 获取文档
     * @param index 索引库
     * @param id 文档Id
     * @throws Exception ex
     */
    public void getDocument(String index,String id) throws Exception{
        GetResponse<UserEntity> getDocResponse = client.get(g -> g
                                                            .index(index)
                                                            .id(id),
                                                        UserEntity.class);
        System.out.printf("索引库(%s) 查询文档Id(%s) 响应结果 -> %s 元数据 -> %s%n",
                index,
                id,
                getDocResponse.found(),
                getDocResponse.source());
    }

    /**
     * 查询文档
     * @param index 索引库
     * @param field 字段
     * @param searchText 查询 keysWord
     * @throws Exception ex
     */
    public void searchDocument(String index,String field,String searchText) throws Exception{
        SearchResponse<UserEntity> searchResponse = client.search(s -> s
                        .index(index)
                        .query(q -> q
                                .match(m -> m
                                        .field(field)
                                        .query(searchText))),
                        UserEntity.class);
        System.out.printf("索引库(%s) 查询字段(%s) 查询条件(%s) 查询结果 -> %s 执行时间(%sms) timeout:%s total:%s MaxScore:%s %n",
                index,
                field,
                searchText,
                searchResponse.hits().hits(),
                searchResponse.took(),
                searchResponse.timedOut(),
                searchResponse.hits().total().value(),
                searchResponse.maxScore());
    }


    /**
     * 查询索引下所有文档
     * @param index 索引
     * @throws Exception 异常
     */
    public void searchAllDocumentByIndex(String index) throws Exception{
        SearchResponse<UserEntity> searchResponse = client.search(s -> s
                .index(index)
                .query(q -> q.matchAll(builder -> builder)),UserEntity.class);
        System.out.printf("索引库(%s) 查询结果 -> %s%n",
                index,
                searchResponse.hits().hits().stream()
                        .map(Hit::source)
                        .collect(Collectors.toList()));
    }

    /**
     * 分页查询
     * @param index 索引
     * @param start 起始文档偏移。必须是非负的,不包前
     * @param size 从起始位置开始获取个数
     * @throws Exception 异常
     */
    public void searchDocumentPage(String index,Integer start,Integer size) throws Exception{
        SearchResponse<UserEntity> searchResponse = client.search(s -> s
                                                        .index(index).from(start).size(size),
                                                    UserEntity.class);
        System.out.printf("索引库(%s) 查询结果 -> %s 耗时(%sms) timeout:%s total:%s MaxScore:%s %n",
                index,
                searchResponse.hits().hits().stream()
                        .map(Hit::source)
                        .collect(Collectors.toList()),
                searchResponse.took(),
                searchResponse.timedOut(),
                searchResponse.hits().total().value(),
                searchResponse.maxScore());
    }


    /**
     * 查询索引下所有文档并排序
     * @param index 索引
     * @param sortMode 排序规则
     * @throws Exception 异常
     */
    public void searchDocSortByIndex(String index, String sortField,SortOrder sortMode) throws Exception {
        SearchResponse<UserEntity> searchResponse = client.search(s -> s
                        .index(index)
                        .query(q -> q.matchAll(builder -> builder))
                        .sort(so -> so.field(f -> f.field(sortField).order(sortMode)))
                , UserEntity.class);
        System.out.printf("索引库(%s) 查询结果 -> %s 耗时:(%sms) timeout:%s total:%s maxScore:%s %n",
                index,
                searchResponse.hits().hits().stream()
                        .map(Hit::source)
                        .collect(Collectors.toList()),
                searchResponse.took(),
                searchResponse.timedOut(),
                searchResponse.hits().total().value(),
                searchResponse.hits().maxScore());
    }


    /**
     * 多条件查询索引下文档
     * @param index 索引
     * @throws Exception 异常
     */
    public void searchDocMultipleConditionsByIndex(String index) throws Exception {
        SearchResponse<UserEntity> searchResponse = client.search(s -> s
                        .index(index)
                        .query(q -> q
                                .bool(b -> b
                                        //查询条件必须匹配，类似于逻辑上的 "AND" 操作。
                                    .must(m -> m.match(t -> t.field("age").query(33)))
                                        //查询条件必须不匹配，类似于逻辑上的 "NOT" 操作。 name !=
                                    .mustNot(mn -> mn.match(t -> t.field("name").query("陈昱任")))
                                        //查询条件是可选的，类似于逻辑上的 "OR" 操作。
                                    .should(sh -> sh.match(t -> t.field("sex").query("男")))

                        ))
                , UserEntity.class);
        System.out.printf("索引库(%s) 查询结果 -> %s 耗时:(%sms) timeout:%s total:%s maxScore:%s %n",
                index,
                searchResponse.hits().hits().stream()
                        .map(Hit::source)
                        .collect(Collectors.toList()),
                searchResponse.took(),
                searchResponse.timedOut(),
                searchResponse.hits().total().value(),
                searchResponse.hits().maxScore());
    }

    /**
     * 范围查询索引下文档
     * @param index 索引
     * @param field 范围查询字段
     * @param beginVal 范围值（开始）
     * @param endVal 范围值（结束）
     * @throws Exception 异常
     */
    public void searchDocRangeByIndex(String index,String field,String beginVal,String endVal) throws Exception {
        SearchResponse<UserEntity> searchResponse = client.search(s -> s
                        .index(index)
                        .query(q -> q
                                .range(r -> r
                                        .field(field)
                                        // 大于等于
                                        .gte(JsonData.of(beginVal))
                                        // 小于等于 40
                                        .lte(JsonData.of(endVal))))
                , UserEntity.class);
        System.out.printf("索引库(%s) 查询结果 -> %s 耗时:(%sms) timeout:%s total:%s maxScore:%s %n",
                index,
                searchResponse.hits().hits().stream()
                        .map(Hit::source)
                        .collect(Collectors.toList()),
                searchResponse.took(),
                searchResponse.timedOut(),
                searchResponse.hits().total().value(),
                searchResponse.hits().maxScore());
    }

    /**
     * 模糊查询索引下文档
     * @param index 索引
     * @param field 模糊匹配字段
     * @param likeVal 查询值
     * @param ambiguityDegree 设置模糊度为 1（即允许一个字符的差异）。
     * @throws Exception 异常
     */
    public void searchDocLikeByIndex(String index,String field,String likeVal,String ambiguityDegree) throws Exception {
        SearchResponse<UserEntity> searchResponse = client.search(s -> s
                        .index(index)
                        .query(q -> q
                                .fuzzy(fz -> fz
                                        .field(field)
                                        .value(likeVal)
                                        .fuzziness(ambiguityDegree)))
                , UserEntity.class);
        System.out.printf("索引库(%s) 查询结果 -> %s 耗时:(%sms) timeout:%s total:%s maxScore:%s %n",
                index,
                searchResponse.hits().hits().stream()
                        .map(Hit::source)
                        .collect(Collectors.toList()),
                searchResponse.took(),
                searchResponse.timedOut(),
                searchResponse.hits().total().value(),
                searchResponse.hits().maxScore());
    }


    /**
     * 高亮查询索引下文档
     * @param index 索引
     * @param field 高亮字段
     * @param val 查询值
     * @throws Exception 异常
     */
    public void searchDocHighlightByIndex(String index,String field,String val) throws Exception {
            SearchResponse<UserEntity> searchResponse = client.search(s -> s
                            // 指定索引库
                            .index(index)
                            .query(q -> q
                                    // 使用match查询
                                    .match(m -> m
                                            //字段名称
                                            .field(field)
                                            //查询关键字
                                            .query(val)
                                    )
                            )
                            .highlight(h -> h
                                    // 替换为你想要高亮的字段
                                    .fields(field, HighlightField.of(hf -> hf))
                                    // 高亮前缀标签
                                    .preTags("<font color='red'>")
                                    // 高亮后缀标签
                                    .postTags("</font>")
                            ),UserEntity.class);
            // 处理查询结果
            for (Hit<UserEntity> hit : searchResponse.hits().hits()) {
                // 输出高亮结果
                Map<String, List<String>> highlight = hit.highlight();
                if (highlight != null) {
                    highlight.forEach((hitItField, fragments) -> {
                        System.out.println("hitItField: " + hitItField);
                        for (String fragment : fragments) {
                            System.out.println("Fragment: " + fragment);
                        }
                    });
                }
            }
        System.out.printf("索引库(%s) 查询结果 -> %s 耗时:(%sms) timeout:%s total:%s maxScore:%s %n",
                index,
                searchResponse.hits().hits().stream()
                        .map(Hit::source)
                        .collect(Collectors.toList()),
                searchResponse.took(),
                searchResponse.timedOut(),
                searchResponse.hits().total().value(),
                searchResponse.hits().maxScore());
    }


    /**
     * 最大值查询索引
     * @param index 索引
     * @param field 求最大值字段
     * @throws Exception 异常
     */
    public void searchDocMaxByIndex(String index,String field) throws Exception {
        String alias = "max" + field;
        SearchResponse<Void> searchResponse = client.search(s -> s
                        // 指定索引库
                        .index(index)
                        .aggregations(alias, a ->
                                a.max(m -> m
                                        .field(field)))
                , Void.class);

        // 获取最大值聚合结果
        double maxAge = searchResponse.aggregations().get(alias).max().value();
        System.out.printf("索引库(%s) 统计结果 -> %s 耗时:(%sms) timeout:%s total:%s maxScore:%s %n",
                index,
                maxAge,
                searchResponse.took(),
                searchResponse.timedOut(),
                searchResponse.hits().total().value(),
                searchResponse.hits().maxScore());
    }

    /**
     * 分组查询索引
     * @param index 索引
     * @param field 求最大值字段
     * @throws Exception 异常
     */
    public void searchDocGroupByIndex(String index,String field) throws Exception {
        //分组别名
        String alias = "group_" + field;
        String keywordField;
        //text 类型字段默认使用的是 analyzed（分析后的）值，而 keyword 类型字段使用的是 not_analyzed（未分析的）值。
        // terms 聚合需要对未分析的字符串进行操作。因此，当你传入 sex.keyword 时，它使用的是 keyword 字段，不会进行分析处理，聚合可以正常工作。
        if (!field.endsWith(".keyword")) {
            keywordField = field + ".keyword";
        }else{
            keywordField = field;
        }
        // 构建聚合查询
        SearchResponse<Void> searchResponse = client.search(s -> s
                        // 指定索引名称
                        .index(index)
                        // 聚合名称
                        .aggregations(alias, a -> a
                                .terms(t -> t.field(keywordField))
                        ),Void.class);
        searchResponse.aggregations().get(alias)
                .sterms()
                .buckets()
                .array()
                .stream().map(bucket -> bucket)
                .forEach(bucket -> System.out.println("key: " + bucket.key().stringValue() + ", Count: " + bucket.docCount()));
    }

    /**
     * 更新文档
     * @param index 索引库
     * @param updateUser 更新数据
     * @throws Exception ex
     */
    public void updateDocument(String index,UserEntity updateUser) throws Exception{
        UpdateResponse<UserEntity> updateResponse = client.update(u -> u
                        .index(index)
                        .id(String.valueOf(updateUser.getId()))
                //需要同时使用 doc() 和 upsert(), doc() 用于更新现有文档，而 upsert() 用于在文档不存在时插入新文档。
                        .doc(updateUser)
                        .upsert(updateUser),
                UserEntity.class);
        System.out.printf("索引库(%s) 更新文档Id(%s) 更新数据(%s) 更新结果 -> %s%n",
                index,
                updateUser.getId(),
                updateUser,
                updateResponse.result().jsonValue());
    }


    /**
     * 删除文档
     * @param index 索引库
     * @param id 文档Id
     * @throws Exception ex
     */
    public void delDocument(String index,String id) throws Exception{
        DeleteResponse deleteResponse = client.delete(d -> d
                .index("user")
                .id(id));
        System.out.printf("索引库(%s) 删除文档Id(%s) 删除结果 -> %s%n",index,id,deleteResponse.result().jsonValue());
    }

    /**
     * 批量删除文档
     * @param index 索引库
     * @param ids 文档Ids
     * @throws Exception ex
     */
    public void batchDelDocument(String index,List<String> ids)throws Exception{
        // 使用Lambda表达式创建BulkRequest
        BulkRequest bulkRequest = new BulkRequest.Builder()
                .operations(
                        ids.stream()
                           .map(id-> new BulkOperation.Builder()
                                    .delete(d -> d.index(index).id(id)).build())
                           .collect(Collectors.toList()))
                .build();
        // 执行批量请求
        BulkResponse bulkResponse = client.bulk(bulkRequest);
        // 检查批量操作的结果
        if (bulkResponse.errors()) {
            bulkResponse.items().forEach(item ->{
                System.out.printf("向索引库(%s) 删除文档Ids:【%s】失败,消耗时间:%s,errorMsg:【%s】%n",
                        index,
                        ids,
                        bulkResponse.took(),
                        Objects.nonNull(item.error())
                                ? item.error().toString()
                                : "");
            });
        } else {
            System.out.printf("向索引库(%s) 删除文档Ids【%s】成功,消耗时间:%s,items:【%s】%n",
                    index,
                    ids,
                    bulkResponse.took(),
                    bulkResponse.items());
        }
    }

}
