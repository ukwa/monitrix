package uk.bl.monitrix.heritrix.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class HeritrixSummary extends AbstractHeritrixResponse {

	private NodeList topLevel;
	
	HeritrixSummary(InputStream xml) throws ParserConfigurationException, SAXException, IOException {
		super(xml);
		topLevel = document.getDocumentElement().getChildNodes();
	}
	
	public String getHeritrixVersion() {
		return getTextContentOfNode("heritrixVersion", topLevel);
	}
	
	public String getJobsDir() {
		return getTextContentOfNode("jobsDir", topLevel);
	}
	
	public HeapReport getHeapReport() {
		return new HeapReport(getChildrenOfNode("heapReport", topLevel));
	}
	
	public List<Job> getJobs() {
		List<Job> jobs = new ArrayList<Job>();
		
		NodeList valueNodes = xPath(document.getDocumentElement(), "//jobs/value");
		if (valueNodes == null)
			return jobs;
		
		for (int i=0; i<valueNodes.getLength(); i++)
			jobs.add(new Job(valueNodes.item(i).getChildNodes()));
		
		return jobs;
	}
	
	public class HeapReport {
		
		private long usedBytes, totalBytes, maxBytes;
		
		HeapReport(NodeList heapReport) {
			this.usedBytes = Long.parseLong(getTextContentOfNode("usedBytes", heapReport));
			this.totalBytes = Long.parseLong(getTextContentOfNode("totalBytes", heapReport));;
			this.maxBytes = Long.parseLong(getTextContentOfNode("maxBytes", heapReport));;
		}
		
		public long getUsedBytes() {
			return usedBytes;
		}
		
		public long getTotalBytes() {
			return totalBytes;
		}
		
		public long getMaxBytes() {
			return maxBytes;
		}
		
	}
	
	public class Job {
		
		private String shortName;
		
		private String url;
		
		private boolean isProfile;
		
		private int launchCount;
		
		private String crawlControllerState;
		
		public Job(NodeList nodes) {
			this.shortName = getTextContentOfNode("shortName", nodes);
			this.url = getTextContentOfNode("url", nodes);
			this.isProfile = Boolean.parseBoolean(getTextContentOfNode("isProfile", nodes));
			this.launchCount = Integer.parseInt(getTextContentOfNode("launchCount", nodes));
			this.crawlControllerState = getTextContentOfNode("crawlControllerState", nodes);
		}

		public String getShortName() {
			return shortName;
		}

		public String getUrl() {
			return url;
		}

		public boolean isProfile() {
			return isProfile;
		}

		public int getLaunchCount() {
			return launchCount;
		}

		public String getCrawlControllerState() {
			return crawlControllerState;
		}
		
	}

}
