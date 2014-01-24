
package at.ac.ait.ubicity.fileloader.util;

import java.io.Serializable;
import java.net.URI;

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
public interface FileCache {
   /**
     * @param uri
     * @return .
     */
    FileInformation getFileInformationFor(final URI uri);

    /**
     * Load cache
     */
    @SuppressWarnings(value = "unchecked")
    void loadCache();

    /**
     * Saves the cache. 
     * It is left to implementing classes to have an own mechanism for saving: 
     * local file system, network, database. Wherever.
     */
    void saveCache();

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
     * @param u
     */
    void setEnabled(final boolean u);

    
    /**
     * If true, weak caching will be enabled.
     *
     * @param w
     */
    void setWeakMode(boolean w);

    
    
    public static class FileInformation  implements Serializable {

        public long lastAccess = System.currentTimeMillis();
        
        public int usageCount = 0;    
        
        //the last detected / observed line count
        public int lineCount = 0;
        
    }
        
}
