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
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

public class CompressedBitSetTermsFilter extends Filter {
    private BitSet bitSet;

    public CompressedBitSetTermsFilter(String base64Compressed) {
        BitSet bs;
        try {
            bs = BitSet.valueOf(Snappy.uncompress(BaseEncoding.base64().decode(base64Compressed)));
        } catch (IOException e) {
            bs = new BitSet();
        }

        this.bitSet = bs;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        List<String> ids = Lists.newArrayList();
        int i = bitSet.nextSetBit(0);

        List<FixedBitSet> docs = Lists.newArrayList();

        if (i != -1) {
            ids.add(String.valueOf(i));
            for (i = bitSet.nextSetBit(i + 1); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                int endOfRun = bitSet.nextClearBit(i);
                do {
                    ids.add(String.valueOf(i));
                    FixedBitSet result = search(context, acceptDocs, ids);
                    docs.add(result);
                    ids.clear();
                }
                while (++i < endOfRun);
            }

            FixedBitSet result = search(context, acceptDocs, ids);
            docs.add(result);
            ids.clear();
        }

        if (!docs.isEmpty()) {
            FixedBitSet result = new FixedBitSet(0);
            for (FixedBitSet bs : docs) {
                result = FixedBitSet.ensureCapacity(result, bs.length());
                result.or(bs);
            }

            return result;
        } else {
            return new FixedBitSet(0);
        }

    }

    private FixedBitSet search(AtomicReaderContext context, Bits acceptDocs, List<String> ids) {
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
