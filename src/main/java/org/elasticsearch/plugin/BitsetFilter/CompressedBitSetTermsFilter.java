package org.elasticsearch.plugin.BitsetFilter;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
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
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

public class CompressedBitSetTermsFilter extends Filter {
    private ImmutableRoaringBitmap bitSet;

    public CompressedBitSetTermsFilter(String base64Compressed) {
        ImmutableRoaringBitmap bs;

        ByteBuffer bb = null;
        try {
            bb = ByteBuffer.wrap(Snappy.uncompress(BaseEncoding.base64().decode(base64Compressed)));
        } catch (IOException e) {
            bb = ByteBuffer.allocate(0);
        }

        bs = new ImmutableRoaringBitmap(bb);

        this.bitSet = bs;
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
        TermsFilter filter = new TermsFilter(UidFieldMapper.NAME, Uid.createTypeUids(Lists.newArrayList("Person"), ids));

        FixedBitSet result;
        try {
            result = (FixedBitSet) filter.getDocIdSet(context, acceptDocs);
        } catch (IOException e) {
            result = new FixedBitSet(0);
        }
        return result;
    }
}
