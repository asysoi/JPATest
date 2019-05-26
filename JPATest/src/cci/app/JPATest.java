package cci.app;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
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
	private final String DB_CONNECTION = "jdbc:oracle:thin:@//localhost:1521/pdborcl";
	private final String DB_USER = "beltpp";
	private final String DB_PASSWORD = "123456";
	
	public static void main(String[] str) throws SQLException {


		AnnotationConfigApplicationContext ctx = 
				new AnnotationConfigApplicationContext();
        ctx.register(AppConfig.class);
        ctx.refresh();
       
        JPATest jpa = (JPATest) ctx.getBean("JPATest");
        jpa.init();
        
        if (true) {
        	jpa.indexCertificates(1000);
        } else {
        	int pagesize = 1000;
        	List<Certificate> certs;
        
        	for (int page=1; page < 899; page++ ) {
        		long start = System.currentTimeMillis();
        		certs = jpa.getCertificatesPage(page, pagesize);
        		// certs = jpa.getCertificateList(page, pagesize);
        		certs.clear();
        		System.out.println(page + ". " + (System.currentTimeMillis() - start) + " | FM: " + Runtime.getRuntime().freeMemory() + " | TM: " + Runtime.getRuntime().totalMemory());
        	}
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
					//List<Product> products = productRepository.getProductsById(cert.getCert_id());
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
				.createQuery(
					    "select cert " +
					    "from Certificate cert")
					.setFirstResult((page-1) * pagesize +1)
					.setMaxResults(pagesize)
					.getResultList();
	}
	
	    
	public void indexCertificates(int pageLimit) throws SQLException {
		PreparedStatement statement = null;
		initConnection();
		
		// String selectTableSQL =
		// "SELECT c.*, p.tovar tovar from C_cert c left join C_PRODUCT_DENORM p on
		// c.cert_id = p.cert_id where rownum < 10000";

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
		String id;

		try {
			for (int j = 1; j < 89; j++) {
				long start = System.currentTimeMillis();
				Certificate cert = null;
				try {
					statement.setString(1, "" + (j * 10000));
					statement.setString(2, "" + ((j - 1) * 10000 + 1));
					rs = statement.executeQuery();
					System.out.print(j + ". " + (System.currentTimeMillis() - start) + " < -----  ");

					int i = 1, row = 1, page = 1;
					
					while (rs.next()) {
						id = rs.getString("CERT_ID");
						batch.put(id, rowMapper.mapRow(rs, row++));
						cert = batch.get(id);
						
						//if (page++ == pageLimit) { 
							 // System.out.println( "    " + row + ". " 
							 //  		  + " -> " + batch.get(id).toString());
						//      page = 1; 
						//}
						 
					}
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
	
}

