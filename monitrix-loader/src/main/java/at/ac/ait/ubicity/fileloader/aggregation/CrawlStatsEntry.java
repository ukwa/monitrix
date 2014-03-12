
package at.ac.ait.ubicity.fileloader.aggregation;

import com.netflix.astyanax.annotations.Component;

/**
 *
 * @author jan van oort
 *
 *
 */
public class CrawlStatsEntry {
    
    /*
    * file_url and stat_ts together build the primary key
    */
    
    @Component( ordinal = 0 )
    String file_url;
    
    @Component( ordinal = 1 )
    long stat_ts;
    
    @Component( ordinal = 2 )
    long downloaded_bytes;
    
    @Component( ordinal = 3 )
    long uris_crawled;
    
    @Component( ordinal = 4 )
    long new_hosts;
    
    @Component( ordinal = 5 )
    long completed_hosts;
    
}
