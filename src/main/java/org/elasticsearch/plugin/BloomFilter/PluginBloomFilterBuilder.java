package org.elasticsearch.plugin.BloomFilter;

import com.google.common.hash.BloomFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseFilterBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PluginBloomFilterBuilder extends BaseFilterBuilder {
    private String lookupFieldName;
    private String lookupId;
    private byte[] serializedBloomFilter;
    private BloomFilter<CharSequence> unserializedBloomFilter;

    public PluginBloomFilterBuilder(String lookupId, String lookupFieldName, BloomFilter<CharSequence> bloomFilter) {
        this.lookupId = lookupId;
        this.lookupFieldName = lookupFieldName;
        this.serializedBloomFilter = null;
        this.unserializedBloomFilter = bloomFilter;
    }

    public PluginBloomFilterBuilder(String lookupId, String lookupFieldName, byte[] serializedBloomFilter) {
        this.lookupId = lookupId;
        this.lookupFieldName = lookupFieldName;
        this.serializedBloomFilter = serializedBloomFilter;
        this.unserializedBloomFilter = null;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PluginBloomFilterParser.NAME);
        builder.field("field", this.lookupFieldName);
        builder.field("id", this.lookupId);

        if (null == serializedBloomFilter && null != unserializedBloomFilter) {
            //serialize
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            unserializedBloomFilter.writeTo(baos);
            builder.field("bf", baos.toByteArray());
        } else {
            builder.field("bf", serializedBloomFilter);
        }

        builder.endObject();
    }
}
