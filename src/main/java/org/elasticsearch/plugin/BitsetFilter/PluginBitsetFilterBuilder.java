package org.elasticsearch.plugin.BitsetFilter;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BaseFilterBuilder;

import java.io.IOException;
import java.util.BitSet;

public class PluginBitsetFilterBuilder extends BaseFilterBuilder {

    private BitSet bitSet;

    public PluginBitsetFilterBuilder(BitSet _bitSet) {
        this.bitSet = _bitSet;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PluginBitsetFilterParser.NAME);
        builder.field("bitSet", this.bitSet);
        builder.endObject();
    }
}
