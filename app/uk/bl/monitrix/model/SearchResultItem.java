package uk.bl.monitrix.model;

/**
 * A search result item
 * @author Rainer Simon <rainer.simon@ait.ac.at>
 *
 */
public class SearchResultItem {
	
	private String title;
	
	private String description;
	
	public SearchResultItem(String title, String description) {
		this.title = title;
		this.description = description;
	}
	
	public String title() {
		return title;
	}
	
	public String description() {
		return description;
	}

}
