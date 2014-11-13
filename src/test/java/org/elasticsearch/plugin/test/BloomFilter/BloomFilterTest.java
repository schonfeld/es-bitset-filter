package org.elasticsearch.plugin.test.BloomFilter;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.plugin.BloomFilter.PluginBloomFilterBuilder;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class BloomFilterTest {

    private static final String INDEX = "myindex";
    private static final String TYPE = "Person";

    private Node node;
    private Client client;

    @Before
    public void before() {
        createElasticsearchClient();
        createIndex();
    }

    @After
    public void after() {
        this.client.close();
        this.node.close();
    }

    /**
     * some productive code
     */
    private String index(final String id, final Map<String,Object> data) {
        // create Client
        final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "mycluster").build();
        final TransportClient tc = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                "localhost", 9300));

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
        final IndexResponse response = tc.prepareIndex(INDEX, TYPE)
                .setId(id)
                .setRefresh(true)
                .setSource(builder)
                .execute()
                .actionGet();

        return response.getId();
    }

    private void createElasticsearchClient() {
        final NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder();
        final ImmutableSettings.Builder settingsBuilder = nodeBuilder.settings();
        settingsBuilder.put("network.publish_host", "localhost");
        settingsBuilder.put("network.bind_host", "localhost");
        settingsBuilder.put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true);

        final Settings settings = settingsBuilder.build();
        this.node = nodeBuilder.clusterName("mycluster").local(false).data(true).settings(settings).node();
        this.client = this.node.client();
    }

    private void createIndex() {
        try {
            this.client.admin().indices()
                    .prepareCreate(INDEX)
                    .setSettings(
                            ImmutableSettings.settingsBuilder()
                                    .put("number_of_shards", 3)
                                    .put("number_of_replicas", "0"))
                    .execute()
                    .actionGet();
        } catch (final IndexAlreadyExistsException e) {
            // index already exists => we ignore this exception
        }
    }

    @Test
    public void test_bloomfilter() throws IOException {
        // do something with elasticsearch
        Set<String> ids = Sets.newHashSet("10", "20", "30");
        for(String id : ids) {
            Map data = Maps.newHashMap();
            data.put("twitter_id", id);
            data.put("following_id", "master");
            index(id, data);
        }

        final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "mycluster").build();
        final TransportClient tc = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                "localhost", 9300));

        Map<String,Object> data = Maps.newHashMap();
        data.put("twitter_id", "master");
        index("master", data);


        BloomFilter bf = new BloomFilter(10, 3.0);
        bf.add("10");
        bf.add("20");

        PluginBloomFilterBuilder filter = new PluginBloomFilterBuilder("master", "following_id", bf);
        SearchResponse searchResponse = tc.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter))
                .execute()
                .actionGet();

        Set<String> expected = Sets.newHashSet("30");
        assert(searchResponse.getHits().getHits().length == expected.size());

        Set<String> resultIds = Sets.newHashSet();
        for (SearchHit hit : searchResponse.getHits()) {
            resultIds.add(hit.getId());
        }

        assert(expected.equals(resultIds));
    }
}
