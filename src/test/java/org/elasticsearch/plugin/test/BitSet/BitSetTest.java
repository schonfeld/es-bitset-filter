package org.elasticsearch.plugin.test.BitSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import org.apache.lucene.queries.TermsFilter;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

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
    private String index(final String id, final String type, final Map<String,Object> data) {
        // create Client
        final Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "mycluster").build();
        final TransportClient tc = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(
                "localhost", 9300));

        // index a document
        final IndexResponse response = tc.prepareIndex("myindex", type)
                .setId(id)
                .setRefresh(true)
                .setSource(data)
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
                    .prepareCreate("myindex")
                    .setSettings(
                            ImmutableSettings.settingsBuilder()
                                    .put("number_of_shards", 3)
                                    .put("number_of_replicas", "0"))
                    .execute()
                    .actionGet();

            XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                    .startObject("properties")
                        //.startObject("followers_bitmap").field("type", "binary").endObject()
                    .endObject();

            PutMappingRequest putMappingRequest = new PutMappingRequest("myindex");
            putMappingRequest.type("Person");
            putMappingRequest.source(mapping);
            this.client.admin().indices().putMapping(putMappingRequest);
        } catch (final IndexAlreadyExistsException e) {
            // index already exists => we ignore this exception
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_bitset() throws IOException {
        final String type = "Person";

        // do something with elasticsearch
        Set<String> ids = Sets.newHashSet("10", "20", "30");
        for(String id : ids) {
            Map data = Maps.newHashMap();
            data.put("twitter_id", id);
            index(id, type, data);
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

        Map data = Maps.newHashMap();
        data.put("twitter_id", "master");
        data.put("followers_bitmap", BaseEncoding.base64().encode(Snappy.compress(bos.toByteArray())));
        index("master", "Person", data);

        String source = "{\"query\":{\"filtered\":{\"filter\":{\"bitset\":{\"index\":\"myindex\",\"type\":\"Person\",\"id\":\"master\",\"field\":\"followers_bitmap\"}}}}}";
        SearchResponse searchResponse = tc.prepareSearch("myindex").setTypes("Person").setSource(source).execute().actionGet();

        assert(searchResponse.getHits().getHits().length == 3);

        //seems like for me the order is reversed, so lets create an assert that is sort agnostic
        Set<String> resultIds = Sets.newHashSet();
        for (SearchHit hit : searchResponse.getHits()) {
            resultIds.add(hit.getId());
        }

        assert(ids.equals(resultIds));
    }
}
