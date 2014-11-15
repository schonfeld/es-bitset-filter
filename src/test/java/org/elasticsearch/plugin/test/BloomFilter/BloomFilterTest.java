package org.elasticsearch.plugin.test.BloomFilter;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.plugin.BloomFilter.PluginBloomFilterBuilder;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.TEST,
        numDataNodes = 0,
        numClientNodes = 0,
        transportClientRatio = 0.0)
public class BloomFilterTest extends ElasticsearchIntegrationTest {

    private static final String INDEX = "myindex";
    private static final String TYPE = "Person";

    protected Settings settingsBuilder() {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "test-cluster")
                .put("network.publish_host", "localhost")
                .put("network.bind_host", "localhost")
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true);

        return builder.build();
    }

    protected void startNode(int nodeOrdinal) {
        Settings settings = settingsBuilder();
        logger.info("--> start node #{}, settings [{}]", nodeOrdinal, settings);
        internalCluster().startNode(settings);

        assertNotNull(client().admin().cluster().prepareState().setMasterNodeTimeout("1s").execute().actionGet().getState().nodes().masterNodeId());
    }

    /**
     * some productive code
     */
    private String index(final String id, final Map<String,Object> data) {

        // turn the data into XContentBuilder
        XContentBuilder builder = null;
        try {
            builder = XContentFactory.contentBuilder(XContentType.SMILE);
            builder.startObject();

            for(String key : data.keySet()) {
                builder.field(key, data.get(key));
            }

            builder.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // index a document
        final IndexResponse response = client().prepareIndex(INDEX, TYPE)
                .setId(id)
                .setRefresh(true)
                .setSource(builder)
                .execute()
                .actionGet();

        return response.getId();
    }

    private void createIndex() {
        int shards = 1;
        try {
            client().admin().indices()
                    .prepareCreate(INDEX)
                    .setSettings(
                            ImmutableSettings.settingsBuilder()
                                    .put("number_of_shards", shards)
                                    .put("number_of_replicas", "0"))
                    .execute()
                    .actionGet();
        } catch (final IndexAlreadyExistsException e) {
            // index already exists => we ignore this exception
        }

        assertEquals(client().admin().indices().prepareStats(INDEX).execute().actionGet().getTotalShards(),shards);
    }

    //must run this test with -da:org.apache.lucene.util.FixedBitSet
    @Test
    public void test_doc_having_greater_max_id_than_results() throws IOException {
        start_one_node_and_index();

        for (int i=0; i<65; i++) {
            String ident = String.format("aunt_evil-%d", i);
            Map<String, Object> data = Maps.newHashMap();
            data.put("twitter_id", ident);
            index(ident, data);

        }

        createOneFullRelationshipTree(65);
        client().admin().indices().prepareOptimize(INDEX).setWaitForMerge(true).setMaxNumSegments(1).execute().actionGet();
        assertEquals(client().admin().indices().prepareStats(INDEX).execute().actionGet().getPrimaries().getSegments().getCount(),1);

        BloomFilter<CharSequence> bf = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 5, 0.03);
        bf.put("10");
        bf.put("20");

        PluginBloomFilterBuilder filter = new PluginBloomFilterBuilder("master", "following_id", bf);
        SearchResponse searchResponse = client().prepareSearch(INDEX)
                .setTypes(TYPE)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter))
                .execute()
                .actionGet();

        Set<String> expected = Sets.newHashSet("30");
        assertEquals(searchResponse.getHits().getHits().length, expected.size());

        Set<String> resultIds = Sets.newHashSet();
        for (SearchHit hit : searchResponse.getHits()) {
            resultIds.add(hit.getId());
        }

        assertEquals(expected,resultIds);


    }

    private void start_one_node_and_index() {
        startNode(1);
        assertEquals(client().admin().cluster().prepareState().execute().actionGet().getState().nodes().size(),1);
        createIndex();
    }

    private void createOneFullRelationshipTree(int additionPreExistingDocs) {
        Map<String, Object> data = Maps.newHashMap();
        data.put("twitter_id", "master");
        index("master", data);

        Set<String> ids = Sets.newHashSet("10", "20", "30");
        for(String id : ids) {
            data.clear();
            data.put("twitter_id", id);
            data.put("following_id", "master");
            index(id, data);
        }

        assertEquals(client().admin().indices().prepareStats(INDEX).execute().actionGet().getTotal().docs.getCount(),4 + additionPreExistingDocs);
    }
    
    @Test
    public void test_bloomfilter() throws IOException {
        start_one_node_and_index();

        createOneFullRelationshipTree(0);

        BloomFilter<CharSequence> bf = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 5, 0.03);
        bf.put("10");
        bf.put("20");

        PluginBloomFilterBuilder filter = new PluginBloomFilterBuilder("master", "following_id", bf);
        SearchResponse searchResponse = client().prepareSearch(INDEX)
                .setTypes(TYPE)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter))
                .execute()
                .actionGet();

        Set<String> expected = Sets.newHashSet("30");
        assertEquals(searchResponse.getHits().getHits().length, expected.size());

        Set<String> resultIds = Sets.newHashSet();
        for (SearchHit hit : searchResponse.getHits()) {
            resultIds.add(hit.getId());
        }

        assertEquals(expected,resultIds);
    }
}
