package org.elasticsearch.plugin.test.BitSet;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.plugins.PluginsService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;

public class BitSetTest {

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
    private String index(final String id, final String json, final String type) {
        // create Client
        final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "mycluster").build();
        final TransportClient tc = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                "localhost", 9300));

        // index a document
        final IndexResponse response = tc.prepareIndex("myindex", type).setId(id).setSource(json).execute().actionGet();
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
            this.client.admin().indices().prepareCreate("myindex").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 3).put("number_of_replicas", "0")).execute().actionGet();
        } catch (final IndexAlreadyExistsException e) {
            // index already exists => we ignore this exception
        }
    }

    @Test
    public void test_bitset() throws IOException {

        // do something with elasticsearch
        final String json = "{\"twitter_id\":\"123\"}";
        final String type = "Person";
        final String id = "123";
        index(id,json, type);

        final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "mycluster").build();
        final TransportClient tc = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                "localhost", 9300));

        BitSet b = new BitSet();
        b.set(123);
        String encode = BaseEncoding.base64().encode(Snappy.compress(b.toByteArray()));

        GetResponse getFields = tc.prepareGet("myindex", "Person", "123").execute().actionGet();

        String source = String.format("{\"query\":{\"filtered\":{\"filter\":{\"bitset\":{\"bitset\":\"%s\"}}}}}", encode);
        SearchResponse searchResponse = tc.prepareSearch("myindex").setTypes("Person").setSource(source).execute().actionGet();

        assert(searchResponse.getHits().getAt(0).getSourceAsString().equals(json));

    }
}
