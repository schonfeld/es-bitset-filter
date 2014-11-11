package org.elasticsearch.plugin.test.BitSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.queryparser.flexible.core.builders.QueryBuilder;
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
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.plugin.BitsetFilter.PluginBitsetFilterBuilder;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.xerial.snappy.Snappy;

import javax.management.Query;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

public class BitSetTest {

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

            XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                    .startObject("properties")
                        .startObject("followers_bitmap").field("type", "binary").endObject()
                    .endObject();

            PutMappingRequest putMappingRequest = new PutMappingRequest(INDEX);
            putMappingRequest.type(TYPE);
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
        // do something with elasticsearch
        Set<String> ids = Sets.newHashSet("10", "20", "30");
        for(String id : ids) {
            Map data = Maps.newHashMap();
            data.put("twitter_id", id);
            index(id, data);
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

        Map<String,Object> data = Maps.newHashMap();
        data.put("twitter_id", "master");
        data.put("followers_bitmap", Snappy.compress(bos.toByteArray()));
        index("master", data);

        PluginBitsetFilterBuilder filter = new PluginBitsetFilterBuilder(INDEX, TYPE, "master", "followers_bitmap");
        SearchResponse searchResponse = tc.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter))
                .execute()
                .actionGet();

        assert(searchResponse.getHits().getHits().length == 3);

        Set<String> resultIds = Sets.newHashSet();
        for (SearchHit hit : searchResponse.getHits()) {
            resultIds.add(hit.getId());
        }

        assert(ids.equals(resultIds));
    }
}
