package cci.model.cert;

import java.util.List;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

@Entity
@Table(name = "c_cert")
public class Certificate {
	@Id
	private Long cert_id;
	private String forms;
	private String unn;
	private String kontrp;
	private String kontrs;
	private String adress;
	private String poluchat;
	private String adresspol;
	private String datacert;
	private String nomercert;
	private String expert;
	private String nblanka;
	private String rukovod;
	private String transport;
	private String marshrut;
	private String otmetka;
	private String stranav;
	private String stranapr;
	private String status;
	private int koldoplist;
	private String flexp;
	private String unnexp;
	private String expp;
	private String exps;
	private String expadress;
	private String flimp;
	private String importer;
	private String adressimp;
	private String flsez;
	private String sez;
	private String flsezrez;
	private String stranap;
	private int otd_id;
	private String otd_name;
	private String otd_addr_index;
	private String otd_addr_city;
	private String otd_addr_line;
	private String otd_addr_building;
	private String parentnumber = "";
	private String parentstatus = "";
	@Transient
	private String tovar;
	private String codestranav;
	private String codestranapr;
	private String codestranap;
	private String category;
	private String eotd_name;
	private String eotd_addr_city;
	private String eotd_addr_line;
	@Transient
	private List<Product> products;
	@Transient
	private String childnumber = "";
	@Transient
	private Integer child_id = 0;
	private Long parent_id;
	@Transient
	private int cursor;
	@Transient
	private int currentlist;
	@Transient
	private String date_load;
	
	
	public String getDate_load() {
		if (date_load != null) {
		   int i = date_load.indexOf("00:00:00.0");
		   return i > 0 ? date_load.substring(0, i) : date_load;
		} else {
		   return null;	
		}
	}

	public void setDate_load(String date_load) {
		this.date_load = date_load;
	}

	public int getCurrentlist() {
		return currentlist;
	}

	public void setCurrentlist(int currentlist) {
		this.currentlist = currentlist;
	}

	public String getTovar() {
		return tovar;
	}

	public void setTovar(String tovar) {
		this.tovar = tovar;
	}

	
	public String getChildnumber() {
		return childnumber;
	}

	public void setChildnumber(String childnumber) {
		this.childnumber = childnumber;
	}

	 
	public Integer getChild_id() {
		return child_id;
	}

	public void setChild_id(Integer child_id) {
		this.child_id = child_id;
	}

	public String getParentnumber() {
		return parentnumber;
	}

	public void setParentnumber(String parentnumber) {
		this.parentnumber = parentnumber;
	}

	public String getParentstatus() {
		return parentstatus;
	}

	public void setParentstatus(String parentstatus) {
		this.parentstatus = parentstatus;
	}

	
	public Long getCert_id() {
		return cert_id;
	}

	public void setCert_id(Long cert_id) {
		this.cert_id = cert_id;
	}

	public String getForms() {
		return forms;
	}

	public void setForms(String forms) {
		this.forms = forms;
	}

	public String getUnn() {
		return unn;
	}

	public void setUnn(String unn) {
		this.unn = unn;
	}

	public String getKontrp() {
		return kontrp;
	}

	public void setKontrp(String kontrp) {
		this.kontrp = kontrp;
	}

	public String getKontrs() {
		return kontrs;
	}

	public void setKontrs(String kontrs) {
		this.kontrs = kontrs;
	}

	public String getAdress() {
		return adress;
	}

	public void setAdress(String adress) {
		this.adress = adress;
	}

	public String getPoluchat() {
		return poluchat;
	}

	public void setPoluchat(String poluchat) {
		this.poluchat = poluchat;
	}

	public String getAdresspol() {
		return adresspol;
	}

	public void setAdresspol(String adresspol) {
		this.adresspol = adresspol;
	}

	public String getDatacert() {
		return datacert;
	}

	public void setDatacert(String datacert) {
		this.datacert = datacert;
	}

	public String getNomercert() {
		return nomercert;
	}

	public void setNomercert(String nomercert) {
		this.nomercert = nomercert;
	}

	public String getExpert() {
		return expert;
	}

	public void setExpert(String expert) {
		this.expert = expert;
	}

	public String getNblanka() {
		return nblanka;
	}

	public void setNblanka(String nblanka) {
		this.nblanka = nblanka;
	}

	public String getRukovod() {
		return rukovod;
	}

	public void setRukovod(String rukovod) {
		this.rukovod = rukovod;
	}

	public String getTransport() {
		return transport;
	}

	public void setTransport(String transport) {
		this.transport = transport;
	}

	public String getMarshrut() {
		return marshrut;
	}

	public void setMarshrut(String marshrut) {
		this.marshrut = marshrut;
	}

	public String getOtmetka() {
		return otmetka;
	}

	public void setOtmetka(String otmetka) {
		this.otmetka = otmetka;
	}

	public String getStranav() {
		return stranav;
	}

	public void setStranav(String stranav) {
		this.stranav = stranav;
	}

	public String getStranapr() {
		return stranapr;
	}

	public void setStranapr(String stranapr) {
		this.stranapr = stranapr;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public int getKoldoplist() {
		return koldoplist;
	}

	public void setKoldoplist(int koldoplist) {
		this.koldoplist = koldoplist;
	}

	public String getFlexp() {
		return flexp;
	}

	public void setFlexp(String flexp) {
		this.flexp = flexp;
	}

	public String getUnnexp() {
		return unnexp;
	}

	public void setUnnexp(String unnexp) {
		this.unnexp = unnexp;
	}

	public String getExpp() {
		return expp;
	}

	public void setExpp(String expp) {
		this.expp = expp;
	}

	public String getExps() {
		return exps;
	}

	public void setExps(String exps) {
		this.exps = exps;
	}

	public String getExpadress() {
		return expadress;
	}

	public void setExpadress(String expadress) {
		this.expadress = expadress;
	}

	public String getFlimp() {
		return flimp;
	}

	public void setFlimp(String flimp) {
		this.flimp = flimp;
	}

	public String getImporter() {
		return importer;
	}

	public void setImporter(String importer) {
		this.importer = importer;
	}

	public String getAdressimp() {
		return adressimp;
	}

	public void setAdressimp(String adressimp) {
		this.adressimp = adressimp;
	}

	public String getFlsez() {
		return flsez;
	}

	public void setFlsez(String flsez) {
		this.flsez = flsez;
	}

	public String getSez() {
		return sez;
	}

	public void setSez(String sez) {
		this.sez = sez;
	}

	public String getFlsezrez() {
		return flsezrez;
	}

	public void setFlsezrez(String flsezrez) {
		this.flsezrez = flsezrez;
	}

	public String getStranap() {
		return stranap;
	}

	public void setStranap(String stranap) {
		this.stranap = stranap;
	}

	public List<Product> getProducts() {
		return products;
	}

	public void setProducts(List<Product> products) {
		this.products = products;
	}

	
	public String getOtd_addr_index() {
		return otd_addr_index;
	}

	public void setOtd_addr_index(String otd_addr_index) {
		this.otd_addr_index = otd_addr_index;
	}

	
	public String getOtd_addr_city() {
		return otd_addr_city;
	}

	public void setOtd_addr_city(String otd_addr_city) {
		this.otd_addr_city = otd_addr_city;
	}

	
	public String getOtd_addr_line() {
		return otd_addr_line;
	}

	public void setOtd_addr_line(String otd_addr_line) {
		this.otd_addr_line = otd_addr_line;
	}

	
	public String getOtd_addr_building() {
		return otd_addr_building;
	}

	public void setOtd_addr_building(String otd_addr_building) {
		this.otd_addr_building = otd_addr_building;
	}

	public String getOtd_name() {
		return otd_name;
	}

	public void setOtd_name(String otd_name) {
		this.otd_name = otd_name;
	}

	
	public String getShort_kontrp() {
		// return kontrp.length() > 100 ? kontrp.substring(1, 100) + " ..." :
		// kontrp;
		return (kontrp != null && kontrp.length() > 100) ? kontrp.substring(1,
				100) + " ..." : kontrp;
	}
	
	
	
	public String getEotd_name() {
		return eotd_name;
	}

	public void setEotd_name(String eotd_name) {
		this.eotd_name = eotd_name;
	}

	
	public String getEotd_addr_city() {
		return eotd_addr_city;
	}

	public void setEotd_addr_city(String eotd_addr_city) {
		this.eotd_addr_city = eotd_addr_city;
	}

	
	public String getEotd_addr_line() {
		return eotd_addr_line;
	}

	public void setEotd_addr_line(String eotd_addr_line) {
		this.eotd_addr_line = eotd_addr_line;
	}

	
	public int getOtd_id() {
		return otd_id;
	}

	public void setOtd_id(int otd_id) {
		this.otd_id = otd_id;
	}

	
	public Long getParent_id() {
		return parent_id;
	}

	public void setParent_id(Long parent_id) {
		this.parent_id = parent_id;
	}

	public String getCodestranav() {
		return codestranav;
	}

	public void setCodestranav(String codestranav) {
		this.codestranav = codestranav;
	}

	public String getCodestranapr() {
		return codestranapr;
	}

	public void setCodestranapr(String codestranapr) {
		this.codestranapr = codestranapr;
	}

	public String getCodestranap() {
		return codestranap;
	}

	public void setCodestranap(String codestranap) {
		this.codestranap = codestranap;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	@Override
	public String toString() {
		return "Certificate [cert_id=" + cert_id + ", datacert=" + datacert + ", nomercert=" + nomercert + ", nblanka="
				+ nblanka + " tovar = " + (tovar == null ? 0 : tovar.length()) + "]";
	}
	
	
	
}
