package org.elasticsearch.plugin.BitsetFilter;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseFilterBuilder;

import java.io.IOException;
import java.util.BitSet;

public class PluginBitsetFilterBuilder extends BaseFilterBuilder {
    private String lookupIndex;
    private String lookupType;
    private String lookupId;
    private String lookupFieldName;

    public PluginBitsetFilterBuilder(String lookupIndex, String lookupType, String lookupId, String lookupFieldName) {
        this.lookupIndex = lookupIndex;
        this.lookupType = lookupType;
        this.lookupId = lookupId;
        this.lookupFieldName = lookupFieldName;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PluginBitsetFilterParser.NAME);
        builder.field("index", this.lookupIndex);
        builder.field("type", this.lookupType);
        builder.field("id", this.lookupId);
        builder.field("field", this.lookupFieldName);
        builder.endObject();
    }
}
