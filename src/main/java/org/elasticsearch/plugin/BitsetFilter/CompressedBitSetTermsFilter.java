package org.elasticsearch.plugin.BitsetFilter;

import com.google.common.collect.Lists;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class CompressedBitSetTermsFilter extends Filter {
    private ImmutableRoaringBitmap bitSet;
    private Collection<String> types;

    public CompressedBitSetTermsFilter(Collection<String> types, ImmutableRoaringBitmap bitmap) {
        this.bitSet = bitmap;
        this.types = types;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        List<Integer> ids = Lists.newArrayList();
        IntIterator intIterator = bitSet.getIntIterator();
        if (intIterator.hasNext()) {
            ids.add(intIterator.next());
            while (intIterator.hasNext()) {
                ids.add(intIterator.next());
            }
        }

        FixedBitSet result;
        if (!ids.isEmpty()) {
             result = search(context, acceptDocs, ids);
        } else {
            result = null;
        }

        return result;
    }

    private FixedBitSet search(AtomicReaderContext context, Bits acceptDocs, List<Integer> ids) {
        TermsFilter filter = new TermsFilter(UidFieldMapper.NAME, Uid.createTypeUids(types, ids));

        FixedBitSet result;
        try {
            result = (FixedBitSet) filter.getDocIdSet(context, acceptDocs);
        } catch (IOException e) {
            result = new FixedBitSet(0);
        }
        return result;
    }
}
