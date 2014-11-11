package org.elasticsearch.plugin.test.BitSet;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
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
    private String index(final String json, final String type) {
        // create Client
        final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "mycluster").build();
        final TransportClient tc = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                "localhost", 9300));

        // index a document
        final IndexResponse response = tc.prepareIndex("myindex", type).setId("1235").setSource(json).execute().actionGet();
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
            this.client.admin().indices().prepareCreate("myindex").execute().actionGet();
        } catch (final IndexAlreadyExistsException e) {
            // index already exists => we ignore this exception
        }
    }

    @Test
    public void test_bitset() {

        // do something with elasticsearch
        final String json = "{\"mytype\":\"bla\"}";
        final String type = "mytype";
        final String id = index(json, type);

        final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "mycluster").build();
        final TransportClient tc = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                "localhost", 9300));

        SearchRequest people = tc.prepareSearch("myindex").setSource("{\n" +
                "\"query\": {\n" +
                "\"filtered\": {\n" +
                "\"filter\": {\n" +
                "\"bitset\": {\n" +
                "\"bitset\": \"test\"\n" +
                "}\n" +
                "}\n" +
                "}\n" +
                "}").request();

        //tc.search(people).actionGet();

        GetRequest g = tc.prepareGet("myindex", "mytype", "1235").request();

        GetResponse getFields = tc.get(g).actionGet();
        int i = 1;

    }
}
