package org.elasticsearch.plugin.BloomFilter;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.plugins.AbstractPlugin;


public class BloomFilterPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "bloom-filter";
    }

    @Override
    public String description() {
        return "Bloom Filter";
    }

    @Override
    public void processModule(Module module) {
        if(module instanceof IndexQueryParserModule) {
            ((IndexQueryParserModule) module).addFilterParser("bloom", PluginBloomFilterParser.class);
        }
    }
}
