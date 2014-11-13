package org.elasticsearch.plugin.BloomFilter;

import com.clearspring.analytics.stream.membership.BloomFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseFilterBuilder;

import java.io.IOException;

public class PluginBloomFilterBuilder extends BaseFilterBuilder {
    private String lookupFieldName;
    private String lookupId;
    private BloomFilter bloomFilter;

    public PluginBloomFilterBuilder(String lookupId, String lookupFieldName, BloomFilter bloomFilter) {
        this.lookupId = lookupId;
        this.lookupFieldName = lookupFieldName;
        this.bloomFilter = bloomFilter;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PluginBloomFilterParser.NAME);
        builder.field("field", this.lookupFieldName);
        builder.field("id", this.lookupId);
        builder.field("bf", BloomFilter.serialize(bloomFilter));
        builder.endObject();
    }
}
