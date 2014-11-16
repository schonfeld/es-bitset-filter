package org.elasticsearch.plugin.BloomFilter;

import com.google.common.hash.BloomFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PluginBloomFilterBuilder extends BaseFilterBuilder {
    private byte[] serializedBloomFilter;
    private BloomFilter<CharSequence> unserializedBloomFilter;
    private FilterBuilder filter;

    public PluginBloomFilterBuilder(BloomFilter<CharSequence> bloomFilter, FilterBuilder filter) {
        this.filter = filter;
        this.serializedBloomFilter = null;
        this.unserializedBloomFilter = bloomFilter;
    }

    public PluginBloomFilterBuilder(byte[] serializedBloomFilter, FilterBuilder filter) {
        this.filter = filter;
        this.serializedBloomFilter = serializedBloomFilter;
        this.unserializedBloomFilter = null;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PluginBloomFilterParser.NAME);
        builder.field("primary", filter);

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
