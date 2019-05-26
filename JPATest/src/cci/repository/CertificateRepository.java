package cci.repository;


import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import cci.model.cert.Certificate;

@Repository
public interface CertificateRepository extends JpaRepository<Certificate, Long> {

}


