package org.elasticsearch.plugin.BitsetFilter;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.plugins.AbstractPlugin;


public class BitsetFilterPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "bitset-filter";
    }

    @Override
    public String description() {
        return "Bitset Filter";
    }

    @Override
    public void processModule(Module module) {
        if(module instanceof IndexQueryParserModule) {
            ((IndexQueryParserModule) module).addFilterParser("bitset", PluginBitsetFilterParser.class);
        }
    }
}
