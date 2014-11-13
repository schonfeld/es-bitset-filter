package org.elasticsearch.plugin.BloomFilter;

import com.clearspring.analytics.stream.membership.BloomFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseFilterBuilder;

import java.io.IOException;

public class PluginBloomFilterBuilder extends BaseFilterBuilder {
    private String lookupFieldName;
    private String lookupId;
    private byte[] serializedBloomFilter;

    public PluginBloomFilterBuilder(String lookupId, String lookupFieldName, BloomFilter bloomFilter) {
        this.lookupId = lookupId;
        this.lookupFieldName = lookupFieldName;
        this.serializedBloomFilter = BloomFilter.serialize(bloomFilter);
    }

    public PluginBloomFilterBuilder(String lookupId, String lookupFieldName, byte[] serializedBloomFilter) {
        this.lookupId = lookupId;
        this.lookupFieldName = lookupFieldName;
        this.serializedBloomFilter = serializedBloomFilter;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PluginBloomFilterParser.NAME);
        builder.field("field", this.lookupFieldName);
        builder.field("id", this.lookupId);
        builder.field("bf", serializedBloomFilter);
        builder.endObject();
    }
}
