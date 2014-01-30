package controllers.mapping;

import java.util.AbstractList;
import java.util.List;

import uk.bl.monitrix.heritrix.api.HeritrixSummary;
import uk.bl.monitrix.heritrix.api.HeritrixSummary.HeapReport;
import uk.bl.monitrix.heritrix.api.HeritrixSummary.Job;

public class HeritrixSummaryMapper {
	
	public String heritrix_version;
	
	public String jobs_dir;

	public HeapReportMapper heap_report;
	
	public List<JobMapper> jobs;
	
	public HeritrixSummaryMapper(HeritrixSummary summary) {
		this.heritrix_version = summary.getHeritrixVersion();
		this.jobs_dir = summary.getJobsDir();
		this.heap_report = new HeapReportMapper(summary.getHeapReport());
		
		// *sigh*
		final List<Job> jobList = summary.getJobs();
		
		this.jobs = new AbstractList<JobMapper>() {
			@Override
			public JobMapper get(int index) {
				return new JobMapper(jobList.get(index));
			}

			@Override
			public int size() {
				return jobList.size();
			}
		};
	}
	
	class HeapReportMapper {
		
		public long used_bytes;
		
		public long total_bytes;
		
		public long max_bytes;
		
		HeapReportMapper(HeapReport report) {
			this.used_bytes = report.getUsedBytes();
			this.total_bytes = report.getTotalBytes();
			this.max_bytes = report.getMaxBytes();
		}
		
	}
	
	class JobMapper {
		
		public String short_name;
		
		public String url;
		
		public boolean is_profile;
		
		public int launch_count;
		
		public String crawl_controller_state;
		
		JobMapper(Job job) {
			this.short_name = job.getShortName();
			this.url = job.getUrl();
			this.is_profile = job.isProfile();
			this.launch_count = job.getLaunchCount();
			this.crawl_controller_state = job.getCrawlControllerState();
		}
		
	}
	
}
