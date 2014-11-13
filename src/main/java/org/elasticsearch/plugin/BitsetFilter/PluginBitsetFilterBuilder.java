package org.elasticsearch.plugin.BitsetFilter;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseFilterBuilder;

import java.io.IOException;

public class PluginBitsetFilterBuilder extends BaseFilterBuilder {
    private String lookupIndex;
    private String lookupType;
    private String lookupId;
    private String bloomFilterBase64;

    public PluginBitsetFilterBuilder(String lookupIndex, String lookupType, String lookupId, String bloomFilterBase64) {
        this.lookupIndex = lookupIndex;
        this.lookupType = lookupType;
        this.lookupId = lookupId;
        this.bloomFilterBase64 = bloomFilterBase64;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PluginBitsetFilterParser.NAME);
        builder.field("index", this.lookupIndex);
        builder.field("type", this.lookupType);
        builder.field("id", this.lookupId);
        builder.field("bf", this.bloomFilterBase64);
        builder.endObject();
    }
}
