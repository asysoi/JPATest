package cci.repository;


import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import cci.model.cert.Certificate;
import cci.model.cert.Product;

@Repository
public interface CertificateRepository extends JpaRepository<Certificate, Long> {

	@Query(value = "select c from c_cert where c.cert_id = :ids", nativeQuery = true)
	List<Certificate> getCertificatesById(@Param("ids") Long ids);
}


