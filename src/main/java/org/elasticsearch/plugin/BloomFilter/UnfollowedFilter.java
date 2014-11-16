package org.elasticsearch.plugin.BloomFilter;

import com.google.common.collect.Sets;
import com.google.common.hash.BloomFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;

public class UnfollowedFilter extends Filter {
    private BloomFilter<CharSequence> bf;
    private Filter primaryFilter;

    public UnfollowedFilter(BloomFilter<CharSequence> bf, Filter primaryFilter) {
        this.bf = bf;
        this.primaryFilter = primaryFilter;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        DocIdSet docIdSet = primaryFilter.getDocIdSet(context, acceptDocs);
        if (null == docIdSet) {
            return null;
        }

        DocIdSetIterator iterator = docIdSet.iterator();
        int docId;
        AtomicReader reader = context.reader();
        FixedBitSet result = new FixedBitSet(reader.maxDoc());

        while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            Document document = reader.document(docId, Sets.newHashSet("_uid"));
            Uid uid = Uid.createUid(document.getField("_uid").stringValue());
            if (!bf.mightContain(uid.id())) {
                result.set(docId);
            }
        }

        return result;
    }
}
