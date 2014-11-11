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
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
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
        final IndexResponse response = tc.prepareIndex("myindex", type).setId(id).setSource(json).setRefresh(true).execute().actionGet();
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
        final String type = "Person";

        // do something with elasticsearch
        List<String> ids = Lists.newArrayList("10", "20", "30");
        for(String id : ids) {
            String json = String.format("{\"twitter_id\":\"%s\"}", id);
            index(id, json, type);
        }

        final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "mycluster").build();
        final TransportClient tc = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                "localhost", 9300));

        MutableRoaringBitmap b = MutableRoaringBitmap.bitmapOf();
        for(String id : ids) {
            b.add(Integer.valueOf(id));
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        b.serialize(dos);
        dos.close();

        String encode = BaseEncoding.base64().encode(Snappy.compress(bos.toByteArray()));

        GetResponse getFields = tc.prepareGet("myindex", "Person", "10").execute().actionGet();

        String source = String.format("{\"query\":{\"filtered\":{\"filter\":{\"bitset\":{\"bitset\":\"%s\"}}}}}", encode);
        SearchResponse searchResponse = tc.prepareSearch("myindex").setTypes("Person").setSource(source).execute().actionGet();

        assert(searchResponse.getHits().getHits().length == 3);
        assert(searchResponse.getHits().getAt(0).getId().equals("10"));
        assert(searchResponse.getHits().getAt(1).getId().equals("20"));
        assert(searchResponse.getHits().getAt(2).getId().equals("30"));
    }
}
