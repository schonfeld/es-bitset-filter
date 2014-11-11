package org.elasticsearch.plugin.BitsetFilter;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DocIdBitSet;

import java.io.IOException;
import java.util.BitSet;

public class PluginBitsetLuceneFilter extends Filter {
    private final BitSet bitSet;

    public PluginBitsetLuceneFilter(BitSet _bitSet) {
        this.bitSet = _bitSet;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        return new DocIdBitSet(this.bitSet);
    }
}
