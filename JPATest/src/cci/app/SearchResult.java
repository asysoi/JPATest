package cci.app;

import java.util.List;

public class SearchResult {
    private int numFoundDocs;
    private List<String> ids;
    private int pageNumber;
    private int hitsPerPage;
	public int getNumFoundDocs() {
		return numFoundDocs;
	}
	public void setNumFoundDocs(int numFoundDocs) {
		this.numFoundDocs = numFoundDocs;
	}
	public List<String> getIds() {
		return ids;
	}
	public void setIds(List<String> ids) {
		this.ids = ids;
	}
	public int getPageNumber() {
		return pageNumber;
	}
	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}
	public int getHitsPerPage() {
		return hitsPerPage;
	}
	public void setHitsPerPage(int hitsPerPage) {
		this.hitsPerPage = hitsPerPage;
	}
   
}
