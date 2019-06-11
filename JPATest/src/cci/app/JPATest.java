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
	
	private SearchManager smng;
	private Connection dbConnection;
	private EntityManager entityManager;
	
	private final String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private final String DB_CONNECTION = "jdbc:oracle:thin:@//192.168.0.179:1521/orclpdb";
	private final String DB_USER = "beltpp";
	private final String DB_PASSWORD = "123456";
	
	public static void main(String[] str) throws Exception {
		String indexPath = "e:\\java\\tmp\\indcert";
		boolean searchOrIndex = true;
		boolean jdbcOrJPA = true;
		AnnotationConfigApplicationContext ctx = 
				new AnnotationConfigApplicationContext();
        ctx.register(AppConfig.class);
        ctx.refresh();
       
        JPATest jpa = (JPATest) ctx.getBean("JPATest");
        SearchManager smng = new SearchManager();
        
        jpa.init();
		long cstart = System.currentTimeMillis();
		
		// jpa.searchBeltpp();
		
		
		if (searchOrIndex) { // search
			Map<String, List<String>> result = smng.search(indexPath, "нефтяной", 1    , 10);
//			List<Certificate> certs = jpa.getCertificatesByIds(ids);
//			for (Certificate cert : certs ) {
//			    System.out.println(cert.getCert_id() + " | " + cert.getNomercert() + " | " + cert.getNblanka() + " | ");        	
//			}
			String rows = (String) result.keySet().toArray()[0];
			System.out.println("Rows : " + rows);
			
			for (String id : result.get(rows) ) {
   			     Certificate cert = jpa.findCertificateByID(id);
   			     System.out.println(cert.getCert_id() + " | " + cert.getNomercert() + " | " + cert.getNblanka() + " | ");   			     
			}
		} else { // index
			if (jdbcOrJPA) {
				jpa.indexCertificates(indexPath, 10000, false);
			} else {
				int pagesize = 10000;
				List<Certificate> certs;

				for (int page = 1; page < 92; page++) {
					long start = System.currentTimeMillis();
					System.out.print(page);
					certs = jpa.getCertificatesPage(page, pagesize);
					System.out.println(certs.get(1).getCert_id());
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

	private Certificate findCertificateByID(String id) {
		return certRepository.findById(Long.parseLong(id)).get();
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
	
	
	public List<Certificate> getCertificatesByIds(List<String> ids) {
		entityManager.clear();
		String sql = "SELECT c from Certificate c where c.cert_id in (" + ids.toString().replace("[", "").replace("]", "") + ")";
				
		return entityManager
				.createQuery(sql, Certificate.class).getResultList(); 
	}
	    
	public void indexCertificates(String indexPath, int blocksize, boolean create) throws SQLException {
		PreparedStatement statement = null;
		initConnection();

		String selectTableSQL = "SELECT c.*, p.tovar tovar from " 
				+ "(select * from c_cert where cert_id in "
				+ " (select  a.cert_id " + " from (SELECT cert_id FROM (select cert_id from c_cert) "
				+ " where rownum <= ? " + ") a left join (SELECT cert_id FROM (select cert_id from c_cert )"
				+ " where rownum < ? " + ") b on a.cert_id = b.cert_id where b.cert_id is null))   "
				+ " c left join C_PRODUCT_DENORM p on c.cert_id = p.cert_id";

		statement = dbConnection.prepareStatement(selectTableSQL);
		
		BeanPropertyRowMapper<Certificate> rowMapper = 
				new BeanPropertyRowMapper<Certificate>(Certificate.class);
		ResultSet rs;
		Map<String, Certificate> batch = new HashMap<String, Certificate>();
		String id = "";
                
		try {
			int row = 1;
			for (int j = 1; j <= 920000/blocksize; j++) {
				long start = System.currentTimeMillis();
				Certificate cert = null;
				try {
					statement.setString(1, "" + (j * blocksize));
					statement.setString(2, "" + ((j - 1) * blocksize   + 1));
					rs = statement.executeQuery();
					System.out.print(j + ". " + (System.currentTimeMillis() - start) + " < -----  ");

					while (rs.next()) {
						id = rs.getString("CERT_ID");
						batch.put(id, rowMapper.mapRow(rs, row++));
						// batch.put(id, mapRow(rs, row++));
					}
					smng.textAddOrUpdateToIndex(indexPath, batch, create);
					batch.clear();
				} catch (SQLException e) {
					System.out.println(e.getMessage());
				} catch (Exception e) {
					e.printStackTrace();
				} 
				System.out.println((System.currentTimeMillis() - start) + " --- >  Row: " + row);
			}
			System.out.println("Добавлено или обновлено " + row + "  документов (сертификатов) !");
		} finally {
			if (statement != null) {
				statement.close();
			}
			if (dbConnection != null) {
				dbConnection.close();
			}
		}
		
	}
	
	public void searchBeltpp() throws SQLException {
		PreparedStatement statement = null;
		Statement stat = null;
		initConnection();
		ResultSet rs;

		try {

			Map<String, String> names = new HashMap<String, String>();
			String namesSQL = "SELECT * from evaluation ORDER by name";
			stat = dbConnection.createStatement();

			rs = stat.executeQuery(namesSQL);

			while (rs.next()) {
				names.put(rs.getString("name"), "");
			}

			String selectTableSQL = "SELECT * from enterprises where UPPER(name_main) like ? or UPPER(name_short_ru) like ? or UPPER(name_full_ru) like ? ";
			statement = dbConnection.prepareStatement(selectTableSQL);

			int i = 1;
			for (String name : names.keySet()) {

				try {
					statement.setString(1, "%"+name.trim().toUpperCase()+"%");
					statement.setString(2, "%"+name.trim().toUpperCase()+"%");
					statement.setString(3, "%"+name.trim().toUpperCase()+"%");
					rs = statement.executeQuery();

					while (rs.next()) {
						names.replace(name, rs.getInt("numbercard") + "");
						//System.out.println(i++ + ". " + name + "\t" + names.get(name));
					}
					System.out.println(name + "\t" + names.get(name));
					// System.out.println((i++) + ". " + name + ": " + names.get(name));
					
				} catch (SQLException e) {
					System.out.println(e.getMessage());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} finally {
			if (stat != null) {
				stat.close();
			}
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
}

