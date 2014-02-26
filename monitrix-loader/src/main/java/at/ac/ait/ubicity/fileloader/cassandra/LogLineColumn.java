
package at.ac.ait.ubicity.fileloader.cassandra;
/**
    Copyright (C) 2013  AIT / Austrian Institute of Technology
    http://www.ait.ac.at

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see http://www.gnu.org/licenses/agpl-3.0.html
 */
/**
 *
 * @author jan van oort
 */

public enum LogLineColumn  {
    
      ID( 0, "log_id" ), TIMESTAMP( 1, "log_ts" ), HTTP_CODE( 2, "status_code" ),
    DOWNLOAD_SIZE( 3, "downloaded_bytes" ), URL( 4, "uri" ), DISCOVERY_PATH( 5, "discovery_path" ), REFERER( 6, "referer" ), 
    CONTENT_TYPE( 7, "content_type" ), WORKER_THREAD( 8, "worker_thread" ), LOG_DATE( 9, "fetch_ts" ), 
    HASH( 10, "hash" ), ANNOTATIONS( 11, "annotations" ), IP_ADDRESS( 12, "ip_address" ), LINE( 13, "line" ), COARSE_TS( 14, "coarse_ts" ),
    LONG_LOG_TS( 15, "long_log_ts" ),
    //placeholder for "no physical column:
    NONE( 16, "none" );
    
    
    protected int order;
    
    public final String name;

    
    private LogLineColumn( int _order, String _name )   {
        order = _order;
        name = _name;
    }
    
    
    
    /**
     * returns the LogLineColumn that is next, in this enumeration order, or the 
     * NONE column if there isn't a ( physical ) next column
     * @return 
     */
    public final LogLineColumn next()    {
       
        return ( order < ( LogLineColumn.values().length - 2 ) ? LogLineColumn.values()[ order + 1 ] : NONE );
    }
}

