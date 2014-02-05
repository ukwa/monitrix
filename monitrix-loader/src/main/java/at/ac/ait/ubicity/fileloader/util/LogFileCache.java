

package at.ac.ait.ubicity.fileloader.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

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
public final class LogFileCache implements FileCache, Serializable    {




    /** Name of the default cache file */
    protected static final String DEFAULT_CACHE_FILE = "monitrix.logfile.cache";
    
    
    final static Logger logger = Logger.getLogger( LogFileCache.class.getName() );

    
    /** Is the cache enabled ? */
    protected boolean cacheEnabled = true;
    
    /** maps a URI to a FileInformation object */
    protected Map< URI, FileInformation > cacheMap;
    
    
    protected boolean weakMode = false;
    
    /* an alternative place for the cache file */
    protected String cachePath = DEFAULT_CACHE_FILE;


    /* a singleton; in principle,we only ever need one instance */
    private final static FileCache singleton;
    
    
    /**
     * make sure the singleton is always there
     */
    static  {
        logger.setLevel( Level.FINEST );
        singleton = new LogFileCache();
    
    }
    
    
    //here for the singleton pattern
    private LogFileCache()  {
        cacheMap = new HashMap();
        
    }
    
    
    

    /**
     * 
     * @return FileCache - our singleton
     */
    public static final FileCache get() {
        return singleton;
    }

    
    
    /**
     * 
     * @param _uri The key: a URI ( of a file or directory ) we wish to save / cache information for
     * @param _info The value: a FileInformation object we cache with this URI key
     * @return this same FileCache ( "convention over configuration"-like stuff )
     */
    @Override
    public final FileCache updateCacheFor( URI _uri, FileInformation _info ) {
        logger.fine("updating cache for uri " + _uri.toASCIIString() );
        cacheMap.put(_uri, _info );
        return this;
    }
    
    
    @Override
    public final FileInformation getFileInformationFor( final URI uri ) {
        logger.finer("cache request for uri: " + uri.toASCIIString() );
        return cacheMap.get( uri );
    }
    
    
    /**
    * 
    * @return this FileCache; if enabled, its cache map will have been filled; 
    * an empty cache map is carried otherwise.
    */
    @Override
    public FileCache loadCache() {
      if ( ! cacheEnabled ) {
            return this;
        }
        final String cacheFile = (this.cachePath == null) ? DEFAULT_CACHE_FILE : this.cachePath;
        try {
            final FileInputStream fis = new FileInputStream( cacheFile );
            final Object rval;
            ObjectInputStream ois = new ObjectInputStream( fis );
            rval = ois.readObject();
            
            if (rval != null) {
                cacheMap = ( Map< URI, FileInformation >) rval;
            }
        } 
        catch( final NullPointerException | FileNotFoundException npe )   {
            logger.info( "no cache file was found at " + ( (this.cachePath == null) ? DEFAULT_CACHE_FILE : this.cachePath ) + "; starting with a fresh, empty cache" );
            
        }
        catch (final ClassNotFoundException e) {
            logger.warning("Your  cache is outdated, please delete it. It will be regenerated with the next run. The next exception reflects this, so don't be afraid.");
        } 
        catch (final IOException e1) {
            logger.severe("caught an IOException while trying to load cache : IOException : " + e1);
        }
        finally {
            return this;
        }
    }

    
    
    /**
     * 
     * @return this FileCache, which has undergone no state changes.
     */
    @Override
    public FileCache saveCache() {

      if (!this.cacheEnabled) {
            return this;
        }
        String cacheFile = this.cachePath;
        FileOutputStream fos = null;
        ObjectOutputStream oos = null;

        if (cacheFile == null) {
            cacheFile = DEFAULT_CACHE_FILE;
        }
        try {
            
            fos = new FileOutputStream(cacheFile);
            oos = new ObjectOutputStream(fos);
            oos.writeObject(this.cacheMap);
            oos.flush();
        } 
        catch (final FileNotFoundException e) {
            logger.severe( "cache file creation problem : " + e.toString() );
        } 
        catch (final IOException e) {
            logger.warning( "an I/O exception was caught while saving the log file cache : " + e.toString() );
        } 
        finally {
            try {
                oos.close();
                fos.close();
            } 
            catch (Exception e) {
                //swallow this one, we're not gonna do anything with it, the next step is to brutally <null> our resources and carry on
            } 
            finally {
                oos = null;
                fos = null;
                logger.info( "persisted the cache to " + ( new File( cacheFile ) ).toURI()  );
                return this;
            }
        }        
    }

    
    
    @Override
    public void setCachePath( final String _cachePath ) {
        cachePath = _cachePath;
    }

    @Override
    public void setEnabled( final boolean u ) {
        cacheEnabled = u;
    }

    /**
     * 
     * @param w -- boolean: should we function in weak mode or not ? 
     * NOTE: this functionality is currently not implemented / used. 
     */
    @Override
    public void setWeakMode( final boolean w ) {
        weakMode = w;
    }


}
