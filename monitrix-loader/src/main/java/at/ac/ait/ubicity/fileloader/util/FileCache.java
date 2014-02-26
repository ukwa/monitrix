
package at.ac.ait.ubicity.fileloader.util;

import java.io.Serializable;
import java.net.URI;
import java.util.Date;

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
 * An interface to be implemented by any utility playing the role of a FileCache. 
 *
 * @author jan van oort
 */
public interface FileCache {
   /**
     * @param uri where is our actual, serialized file living ? 
     */
    FileInformation getFileInformationFor(final URI uri);

    /**
     * Load cache
     */
    @SuppressWarnings(value = "unchecked")
    public FileCache loadCache();

    
    
    /**
     * Update the cache with the latest info available for a File ( by URI )
     * 
     * @param _uri
     * @param _info
     * @return 
     */
    public FileCache updateCacheFor( URI _uri, FileInformation _info );
    
    
    /**
     * Saves the cache. 
     * It is left to implementing classes to have an own mechanism for saving: 
     * local file system, network, database. Wherever.
     */
    public FileCache saveCache();

    /**
     * @param cachePath - may be a file name only, or a fully qualified name. 
     * If an implementing class decides to save and restore over the network, 
     * the cache path should be a valid URI.
     * 
     * If caching is performed in a database, it is left to the implementor to 
     * make cache path point to e.g. a table, view,column family, node - whatever. 
     */
    void setCachePath( String cachePath );

    
    /**
     * is the cache enabled ? 
     *
     * @param u - whether the cache should be enabled or not
     */
    void setEnabled(final boolean u);

    
    /**
     * If true, weak caching will be enabled.
     *
     * @param w
     */
    void setWeakMode(boolean w);

    
    
    public static class FileInformation  implements Serializable {

        private long lastAccess = System.currentTimeMillis();
        
        private int usageCount = 0;    
        
        //the last detected / observed line count
        private int lineCount = 0;

        private final URI uri; 
        
        public final static String fieldSeparator = " :: ";
        
        
        public FileInformation( URI _uri, long _lastAccess, int _usageCount, int _lineCount ) {
            uri = _uri;
            setLastAccess( _lastAccess );
            setUsageCount( _usageCount );
            setLineCount( _lineCount );
        }
        
        
        
        public final URI getURI()   {
            return uri;
        }
        
        /**
         * @return the lastAccess
         */
        public long getLastAccess() {
            return lastAccess;
        }

        /**
         * @param lastAccess the lastAccess to set
         */
        public final void  setLastAccess(long lastAccess) {
            this.lastAccess = lastAccess;
        }

        /**
         * @return the usageCount
         */
        public int getUsageCount() {
            return usageCount;
        }

        /**
         * @param usageCount the usageCount to set
         */
        public final void setUsageCount(int usageCount) {
            this.usageCount = usageCount;
        }

        /**
         * @return the lineCount
         */
        public int getLineCount() {
            return lineCount;
        }

        /**
         * @param lineCount the lineCount to set
         */
        public final  void setLineCount(int lineCount) {
            this.lineCount = lineCount;
        }
        
        
        @Override
        public final String toString()  {
            return uri.toASCIIString() + fieldSeparator + " last accessed :" + new Date( getLastAccess() )+  fieldSeparator + " usage count :" + getUsageCount() + fieldSeparator + " line count :" + getLineCount() ;
        }
        
        
    } 
        
}
