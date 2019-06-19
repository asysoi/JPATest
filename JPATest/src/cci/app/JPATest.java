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
//	@Autowired
//	private ProductRepository productRepository;
//	@Autowired
//	private CertificateRepository certRepository;
//	@Autowired
//	private EntityManagerFactory emFactory;
	
	private static final Logger LOG = Logger.getLogger(JPATest.class);
	private static final String ID = "id";
	private static final String CONTENT = "content";
	private static final String DATE = "datacert";
	
	private static final IndexManager smng = new IndexManager();
	private Connection dbConnection;
	
	private final String DB_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private final String DB_CONNECTION = "jdbc:oracle:thin:@//192.168.0.179:1521/orclpdb";
	private final String DB_USER = "beltpp";
	private final String DB_PASSWORD = "123456";
	
	public static void main(String[] str) throws Exception {
	
		String indexPath = "e:\\java\\tmp\\indcert2";
		boolean searchOrIndex = true;
		
		//AnnotationConfigApplicationContext ctx = 
		//		new AnnotationConfigApplicationContext();
        //ctx.register(AppConfig.class);
        //ctx.refresh();
       
        // JPATest jpa = (JPATest) ctx.getBean("JPATest");
        JPATest jpa = new JPATest();
		jpa.initConnection();
        // smng = new IndexManager();
        
   		long cstart = System.currentTimeMillis();
		
		if (searchOrIndex) { // search
			int page = 1;
			SearchResult result = null;

			while (page < 3) {
				result = smng.search(indexPath, "моло*", ID, CONTENT, DATE, page++, 10);
				
				System.out.print(page + ". " + (System.currentTimeMillis() - cstart) + " msec. ");
				List<String> ids = new ArrayList<String>();

				int rows = result.getNumFoundDocs();
				System.out.println("Found Rows : " + rows + " in " + (System.currentTimeMillis() - cstart) + " msec");

				long start = System.currentTimeMillis();
				int i = 0;
				
				for (String id : result.getIds()) {
					Certificate cert = jpa.findCertificateByID(id);
					
					System.out.println(
							cert.getCert_id() + " || " + cert.getNomercert() + " || " + cert.getNblanka() + 
							                  " || " + cert.getDatacert() + " <-> " + result.getDates().get(i++));
				}
				System.out.println("Time: " + (System.currentTimeMillis() - start));
			}
		} else { // index
			jpa.indexCertificates(indexPath, 10000, false);
 		}
		
		if (jpa.getDbConnection() != null) {
			jpa.getDbConnection().close();
		}
		
		// ctx.close();
    }

 	/* ***********************************
    * Open connection to database
    ************************************/  
	private void initConnection() {
		try {
			Class.forName(DB_DRIVER);
			dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
		} catch (ClassNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public Connection getDbConnection() {
		return dbConnection;
	}


	/* ***********************************
	 * Get list certificates by list of ids 
	 * ***********************************/
	public List<Certificate> getCertificatesByIds(List<String> ids) throws SQLException {
		List<Certificate> certs = new ArrayList<Certificate>();
		Statement statement = null;

		try {
			String sql = 
			      "select * from CERT_VIEW WHERE cert_id in (" + ids.toString().replace("[", "").replace("]", "") + ")";
			statement = 
					dbConnection.prepareStatement(sql);
			BeanPropertyRowMapper<Certificate> rowMapper = 
					new BeanPropertyRowMapper<Certificate>(Certificate.class);
			ResultSet rs = statement.executeQuery(sql);
			int row = 1;
			while (rs.next()) {
			      certs.add(rowMapper.mapRow(rs, row++));
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
		return certs;
	}

	/* *************************************
	 *  Get single certificate by id
	 * *************************************/
	private Certificate findCertificateByID(String id) throws SQLException {
		Certificate cert = null;
		PreparedStatement statement = null;

		try {
			statement = 
					dbConnection.prepareStatement("select * from CERT_VIEW WHERE cert_id = ?");
			BeanPropertyRowMapper<Certificate> rowMapper = 
					new BeanPropertyRowMapper<Certificate>(Certificate.class);
			statement.setString(1, id);
			ResultSet rs = statement.executeQuery();
			rs.next();
			cert = rowMapper.mapRow(rs, 1);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
		return cert;
	}
    
   /* ***************************
	*  Create fultext index
	* ****************************/ 
	public void indexCertificates(String indexPath, int blocksize, boolean create) throws SQLException {
		PreparedStatement statement = null;

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
		Map<String, Certificate> certs = new HashMap<String, Certificate>();
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
						certs.put(id, rowMapper.mapRow(rs, row++));
					}
					smng.addUpdateIndex(indexPath, certs, ID, CONTENT, DATE, create);
					certs.clear();
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
		}
	}
}

