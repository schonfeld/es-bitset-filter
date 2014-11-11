To Install:

1. Stop local ES

2. Compile a new package, if needed:
mvn package

3. Remove the [old] plugin, if needed:
/usr/local/Cellar/elasticsearch/1.3.4/bin/plugin -remove BitsetFilter

4. Install the plugin:
/usr/local/Cellar/elasticsearch/1.3.4/bin/plugin -url file:///Users/michaelschonfeld/ModernMast/ElasticsearchBitsetFilter/target/releases/BitsetFilter-1.0-SNAPSHOT.zip -install BitsetFilter

5. Start local ES, and query using:
GET _search
{
  "query": {
    "filtered": {
      "filter": {
        "bitset": {
          "bitset": %INTEGER%
        }
      }
    }
  }
}
