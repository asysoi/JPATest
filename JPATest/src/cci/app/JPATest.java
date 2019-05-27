package cci.app;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import cci.config.AppConfig;
import cci.model.cert.Certificate;
import cci.model.cert.Product;
import cci.repository.CertificateRepository;
import cci.repository.ProductRepository;

import org.springframework.data.jpa.repository.support.JpaRepositoryFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public class JPATest {
	@Autowired
	private ProductRepository productRepository;
	@Autowired
	private CertificateRepository certRepository;
	@Autowired
	private EntityManagerFactory emFactory;
	
	private Connection dbConnection;
	private EntityManager entityManager;
	
	private final String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private final String DB_CONNECTION = "jdbc:oracle:thin:@//192.168.0.179:1521/orclpdb";
	private final String DB_USER = "beltpp";
	private final String DB_PASSWORD = "123456";
	
	public static void main(String[] str) throws Exception {
		String indexPath = "e:\\java\\tmp\\indcert";
		AnnotationConfigApplicationContext ctx = 
				new AnnotationConfigApplicationContext();
        ctx.register(AppConfig.class);
        ctx.refresh();
       
        JPATest jpa = (JPATest) ctx.getBean("JPATest");
        jpa.init();
		long cstart = System.currentTimeMillis();
		
		if (true) { // search
			jpa.search(indexPath, "+by +sea");
		} else { // index
			if (true) {
				jpa.indexCertificates(indexPath, 10000);
			} else {
				int pagesize = 1000;
				List<Certificate> certs;

				for (int page = 1; page < 899; page++) {
					long start = System.currentTimeMillis();
					System.out.print(page);
					certs = jpa.getCertificatesPage(page, pagesize);
					// certs = jpa.getCertificateList(page, pagesize);
					certs.clear();
					System.out.println(". " + (System.currentTimeMillis() - start) + " | FM: "
							+ Runtime.getRuntime().freeMemory() + " | TM: " + Runtime.getRuntime().totalMemory());
				}
			}
			System.out.println("Loaded in :" + (System.currentTimeMillis() - cstart));
		}
		ctx.close();
    }

	public void init () {
		entityManager = emFactory.createEntityManager();
		JpaRepositoryFactory jpaRepositoryFactory = 
        		new JpaRepositoryFactory(entityManager);
        certRepository = jpaRepositoryFactory.getRepository(CertificateRepository.class);
	}
	
	
	public List<Certificate> getCertificateList(int page, int pagesize) {
		List<Certificate> certs = new ArrayList<Certificate>();
		try {
			for (long i = (page-1)*pagesize + 1; i < page*pagesize; i++) {
				try {
					Certificate cert = certRepository.findById(i).get();
					//ist<Product> products = productRepository.getProductsById(cert.getCert_id());
					//cert.setProducts(products);
					certs.add(cert);
				} catch (Exception ex) {
					// Ignore no id
				}
			}
		} catch (Exception ex) { 
			ex.printStackTrace();
		}
		return certs;
	}
	
	public List<Certificate> getCertificatesPage(int page, int pagesize) {
		entityManager.clear();
		return entityManager
				.createNativeQuery(
					// "select cert from Certificate cert")
					"SELECT * from c_cert") 	
					.setFirstResult((page-1) * pagesize +1)
					.setMaxResults(pagesize)
					.getResultList();
	}
	
	    
	public void indexCertificates(String indexPath, int blocksize) throws SQLException {
		PreparedStatement statement = null;
		initConnection();

		String selectTableSQL = "SELECT c.*, p.tovar tovar from " 
				+ "(select * from c_cert where cert_id in "
				+ " (select  a.cert_id " + " from (SELECT cert_id FROM (select cert_id from c_cert) "
				+ " where rownum <= ? " + ") a left join (SELECT cert_id FROM (select cert_id from c_cert )"
				+ " where rownum <= ? " + ") b on a.cert_id = b.cert_id where b.cert_id is null))   "
				+ " c left join C_PRODUCT_DENORM p on c.cert_id = p.cert_id";

		statement = dbConnection.prepareStatement(selectTableSQL);
		
		BeanPropertyRowMapper<Certificate> rowMapper = new BeanPropertyRowMapper<Certificate>(
				Certificate.class);
		ResultSet rs;
		Map<String, Certificate> batch = new HashMap<String, Certificate>();
		String id = "";
                
		try {
			for (int j = 1; j < 890000/blocksize; j++) {
				long start = System.currentTimeMillis();
				Certificate cert = null;
				try {
					statement.setString(1, "" + (j * blocksize));
					statement.setString(2, "" + ((j - 1) * blocksize   + 1));
					rs = statement.executeQuery();
					System.out.print(j + ". " + (System.currentTimeMillis() - start) + " < -----  ");

					int i = 1, row = 1, page = 1;
					
					while (rs.next()) {
						id = rs.getString("CERT_ID");
						batch.put(id, rowMapper.mapRow(rs, row++));
						// batch.put(id, mapRow(rs, row++));
					}
					textAddOrUpdateToIndex(indexPath, batch, false);
					batch.clear();
				} catch (SQLException e) {
					System.out.println(e.getMessage());
				} catch (Exception e) {
					e.printStackTrace();
				} 
				System.out.println((System.currentTimeMillis() - start) + " --- > ");
			}
		} finally {
			if (statement != null) {
				statement.close();
			}
			if (dbConnection != null) {
				dbConnection.close();
			}
		}
		
	}
	
	
	private void initConnection() {
		try {
			Class.forName(DB_DRIVER);
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		}

		try {
			dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	private Certificate mapRow(ResultSet rs, int row) throws SQLException {
		Certificate cert = new Certificate();
		cert.setCert_id(rs.getLong("cert_id"));
		cert.setNomercert(rs.getString("nomercert"));
		cert.setNblanka(rs.getString("nblanka"));
		cert.setTovar(rs.getString("tovar"));
		return cert;
	}
	
   /* -------------------------------------------
    * Add list of Certificates to Lucene index 
	* ------------------------------------------- */	
	private void textAddOrUpdateToIndex(String indexPath, Map batch, Boolean create) throws Exception {
		IndexWriter writer = null;
		Long start = System.currentTimeMillis();		

		try {
			Directory dir = FSDirectory.open(Paths.get(indexPath));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			iwc.setOpenMode(create ? OpenMode.CREATE : OpenMode.CREATE_OR_APPEND);
			iwc.setRAMBufferSizeMB(256.0);

			writer = new IndexWriter(dir, iwc);
			Set<String> ids = batch.keySet();

			for (String id : ids) {
				if (batch.get(id) != null) {
					Document doc = new Document();
					doc.add(new StringField("id", id, Field.Store.YES));
					doc.add(new TextField("content", batch.get(id).toString(), Field.Store.NO));
					
					if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
						writer.addDocument(doc);
					} else {
						writer.updateDocument(new Term("id", id), doc);
					}
				}
			}
			// writer.forceMerge(1);
		} catch (Exception e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		} finally {
		   if (writer != null) writer.close();
		}
		System.out.println(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date())+ " - Added to index - " + (System.currentTimeMillis() - start));
	}
	
   /* -------------------------------------------
	* Search by Lucene index 
	* ------------------------------------------- */	
	public void search(String index, String queryString) throws Exception {
		String field = "content";
		String queries = null;
		boolean raw = false;
		int hitsPerPage = 10;
		int start = 1;
		
		QueryParser parser = new QueryParser(field, new StandardAnalyzer());
		Query query = parser.parse(queryString);
		System.out.println("Searching for: " + query.toString(field));
		
		IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
		IndexSearcher searcher = new IndexSearcher(reader);
		//TopDocs results = searcher.search(query, hitsPerPage);
		
		TopDocs results = searcher.search(query, reader.numDocs()); 
		ScoreDoc[] hits = results.scoreDocs;

		int numTotalHits = Math.toIntExact(results.totalHits.value);
		System.out.println(numTotalHits + " total matching documents");
		
		for(int i=start; i<start+hitsPerPage && i < numTotalHits; ++i) {
		    int docId = hits[i].doc;
		    Document d = searcher.doc(docId);
		    System.out.println(i + ". " +  d.get("id") + " | " + d.get("content"));
		}
		if (reader != null) {
			System.out.println("Documents: " + reader.numDocs());
			reader.close();
		}
	}

}

