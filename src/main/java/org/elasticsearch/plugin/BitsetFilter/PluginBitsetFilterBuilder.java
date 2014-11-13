package org.elasticsearch.plugin.BitsetFilter;

import com.clearspring.analytics.stream.membership.BloomFilter;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseFilterBuilder;

import java.io.IOException;

public class PluginBitsetFilterBuilder extends BaseFilterBuilder {
    private String lookupFieldName;
    private String lookupId;
    private BloomFilter bloomFilter;

    public PluginBitsetFilterBuilder(String lookupId, String lookupFieldName, BloomFilter bloomFilter) {
        this.lookupId = lookupId;
        this.lookupFieldName = lookupFieldName;
        this.bloomFilter = bloomFilter;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PluginBitsetFilterParser.NAME);
        builder.field("field", this.lookupFieldName);
        builder.field("id", this.lookupId);
        builder.field("bf", BloomFilter.serialize(bloomFilter));
        builder.endObject();
    }
}
