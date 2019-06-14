package cci.app;


import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.stereotype.Component;

@Component
public class SearchManager {
	private static final Logger LOG = Logger.getLogger(SearchManager.class);
    
   /* -------------------------------------------
    * Add list of documents to Lucene index 
    * docs - Map<String id, String content> 
    *  
	* ------------------------------------------- */	
	public void addUpdateIndex(String indexPath, Map docs, String idFieldName, String contentFieldName, boolean create) throws Exception {
		IndexWriter writer = null;
		Long start = System.currentTimeMillis();		

		try {
			Directory dir = FSDirectory.open(Paths.get(indexPath));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(create ? OpenMode.CREATE : OpenMode.CREATE_OR_APPEND);
			iwc.setRAMBufferSizeMB(256.0);

			writer = new IndexWriter(dir, iwc);
			Set<String> ids = docs.keySet();

			for (String id : ids) {
				if (docs.get(id) != null) {
					Document doc = new Document();
					doc.add(new StringField(idFieldName, id, Field.Store.YES));
					doc.add(new TextField(contentFieldName, docs.get(id).toString(), Field.Store.NO));
					
					if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
						writer.addDocument(doc);
					} else {
						writer.updateDocument(new Term(idFieldName, id), doc);
					}
				}
			}
			// writer.forceMerge(1);
		} catch (Exception e) {
			LOG.info(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		} finally {
		   if (writer != null) writer.close();
		}
		LOG.info(new SimpleDateFormat("HH:mm:ss").format(new Date())+ " - Added to index - " + (System.currentTimeMillis() - start) + " ---- ");
	}
	
   /* -----------------------------------------------------
	* Search by Lucene index 
	* 
	* return result as Map<String, List<String>>
	* String - number found rows 
	* List<String> - list of found id selected result page
	* ---------------------------------------------------- */	
	public SearchResult search(String indexPath, String queryString, int numberPage, int hitsPerPage, String idFieldName, String contentFieldName) throws Exception {
		String queries = null;
		boolean raw = false;
	
		QueryParser parser = new QueryParser(contentFieldName, new StandardAnalyzer());
		Query query = parser.parse(queryString);
		IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
		IndexSearcher searcher = new IndexSearcher(reader);
		TopDocs results = searcher.search(query, reader.numDocs()); 
		ScoreDoc[] hits = results.scoreDocs;
           
		int numTotalHits = Math.toIntExact(results.totalHits.value);
		LOG.info(numTotalHits + " total matching documents");
		List<String> ids = new ArrayList();
		
		for(int i=(numberPage - 1)*hitsPerPage + 1; i<=numberPage * hitsPerPage && i < numTotalHits; ++i) {
		    int docId = hits[i].doc;
		    Document d = searcher.doc(docId);
		    ids.add(d.get(idFieldName));
		    LOG.info(i + ". " +  d.get("id") + " | " + hits[i].score +  " | " +  d.get(contentFieldName) );
		}
		if (reader != null) {
			LOG.info("Documents: " + reader.numDocs());
			reader.close();
		}
		SearchResult result =  new SearchResult();
		result.setNumFoundDocs(numTotalHits);
		result.setIds(ids);
		result.setHitsPerPage(hitsPerPage);
		result.setPageNumber(numberPage);
		   
		return result;
	}
}
