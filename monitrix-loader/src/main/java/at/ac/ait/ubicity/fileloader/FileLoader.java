package at.ac.ait.ubicity.fileloader;

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


import at.ac.ait.ubicity.fileloader.cassandra.AstyanaxInitializer;
import at.ac.ait.ubicity.fileloader.util.Delay;
import at.ac.ait.ubicity.fileloader.util.FileCache;
import at.ac.ait.ubicity.fileloader.util.FileCache.FileInformation;
import at.ac.ait.ubicity.fileloader.util.LogFileCache;
import at.ac.ait.ubicity.fileloader.util.LogFileNameFilter;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;

import java.io.File;
import java.io.FileFilter;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;




/**
 *
 * @author Jan van Oort
 */

public final class FileLoader {
    

    public final static double TWO = 2.0;
    
    final static Logger logger = Logger.getLogger( "FileLoader" );
    
    static Keyspace keySpace;

    static boolean useCache = true;
    
    //delay, in milliseconds, for which to check our invigilance directory or file for new updates
    static final long INVIGILANCE_WAITING_DELAY = 5000;
    
    static  {
        logger.setLevel( Level.ALL );
    }
    
    @SuppressWarnings("unchecked")
    public final static void load( final FileInformation _fileInfo, final String _keySpace, final String _host, final int _batchSize ) throws Exception {

        keySpace = AstyanaxInitializer.doInit( "Test Cluster", _host, _keySpace );
        final MutationBatch batch = keySpace.prepareMutationBatch();
        
        logger.info( "got keyspace " + keySpace.getKeyspaceName() + " from Astyanax initializer" );
        
        final LineIterator onLines = FileUtils.lineIterator( new File( _fileInfo.getURI() ) );
        
        final ExecutorService exec = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() * 2 );        
        final Disruptor< SingleLogLineAsString > disruptor = new Disruptor( SingleLogLineAsString.EVENT_FACTORY, ( int ) Math.pow( TWO, 20 ), exec );
        SingleLogLineAsStringEventHandler.batch = batch;
        SingleLogLineAsStringEventHandler.keySpace = keySpace;
        SingleLogLineAsStringEventHandler.batchSize = _batchSize;
        
        final EventHandler< SingleLogLineAsString > handler = new SingleLogLineAsStringEventHandler(  );
        disruptor.handleEventsWith( handler );
        final RingBuffer< SingleLogLineAsString > rb = disruptor.start();
        
        int _lineCount = 0;
        long _start, _lapse;
        _start = System.nanoTime();
        
        
        int _linesAlreadyProcessed = _fileInfo.getLineCount();
        
        //cycle through the lines already processed
        while( _lineCount < _linesAlreadyProcessed ) {
            onLines.nextLine();
            _lineCount++;
        }
        
        //now get down to the work we actually must do
        while( onLines.hasNext() ){
            logger.info( "begin proccessing of file " + _fileInfo.getURI() + " @line #" + _lineCount );
            final long _seq = rb.next();
            final SingleLogLineAsString event = rb.get( _seq );
            event.setValue( onLines.nextLine() );
            rb.publish(_seq);
            _lineCount++;
        }

        
        disruptor.shutdown();
        _lapse = System.nanoTime() - _start;

        //update the file info, this will  land in the cache
        _fileInfo.setLineCount( _lineCount );
        _fileInfo.setLastAccess( System.currentTimeMillis() );
        int _usageCount = _fileInfo.getUsageCount();
        _fileInfo.setUsageCount( _usageCount++ );
        
        
        
        logger.info( "handled " + ( _lineCount - _linesAlreadyProcessed )  + " log lines in " + _lapse + " nanoseconds" );
    }
    
    
    public final static void invigilate( URI _uri, String keySpace, String host, int batchSize )  {
        logger.info( "invigilating URI: " + _uri );
        if( _uri.getScheme().equals( "file" ) ) {
            //we don't know yet if the URI is a directory or a file
            File _startingPoint = new File( _uri );
            File[] _files = null;
            if ( _startingPoint.isDirectory()) {
                _files = _startingPoint.listFiles( new LogFileNameFilter() );
            }
            else    {
                _files = new File[ 1 ];
                _files[ 0 ] = _startingPoint;
            }
            for( File f: _files )   {
                logger.info( "found file under / at URI: " + f.getName() );
            }
            if( useCache )  {
                /**
                 * We are supposed to use a cache. This implies:
                 * 1) go and get the cache, i.e. load it
                 * 2) get all the files at / under the URI
                 * 3) check if there is an entry for any file in the cache
                 *      3a) if so, then attempt to load that file from the specified line counter on
                 *      3b) if not, then load the entire file
                 * 4) update the cache info ( FileCache.FileInformation ) 
                 * 5) save the cache
                 * 6) wait for INVIGILANCE_WAITING_DELAY milliseconds, then start the same process all over again
                 */
                //1) get the cache
                FileCache cache = LogFileCache.get();
                cache.loadCache();
                
                for( File file: _files ) {
                    FileInformation _fileInfo = cache.getFileInformationFor( file.toURI() );
                    if( _fileInfo == null ) {
                        _fileInfo = new FileInformation( file.toURI(), System.currentTimeMillis(), 1, 0 );
                        cache.updateCacheFor( file.toURI(), _fileInfo );
                    }
                    logger.info(_fileInfo.toString()  );
                }
                cache.saveCache();
            }
            else    {
                /**
                 * We're on our own. This implies:
                 * 1) we will need to inspect the url for any log file presence
                 * 2) we'll do a one-off load()
                 * 3) we are not supposed to store any information in a cache, so we can safely return
                 */
            }
            return;
        }
        logger.info( "URI " + _uri.toString() + " is not something FileLoader can currently handle" );
    }
    
    
    
    /**
     * 
     * This method is here for demo purposes only. It is not part of the required functionality for this class. 
     * 
     * 
     * @param args arg 0 = file, arg #1 = keyspace, arg #2 = server host name, arg #3 = batch size
     * ( For now, tacitly assume we are on the default Cassandra 9160 port ). Clustering is not yet supported.
     */
    public final static void main( String[] args )  {
        if(  ! ( args.length == 6 )  )  {
            usage();
            System.exit( 1 );
        }
        try {
            final File _f = new File( args[ 0 ] );
            
            URI uri = _f.toURI();
            String keySpaceName = args[ 1 ];
            final String host = args[ 2 ];
            final int batchSize = Integer.parseInt( args[ 3 ] );
            final int timeUnitCount = Integer.parseInt( args[ 4 ] );
            Delay timeUnit = timeUnitsFromCmdLine( args[ 5 ].toUpperCase()  );
            if( timeUnit == null ) timeUnit = Delay.SECOND;
            long millisToWait = timeUnitCount * timeUnit.getMilliSeconds();
            while( true )   {
                try {
                    invigilate( uri, keySpaceName, host, batchSize );
                    Thread.sleep( millisToWait );
                }
                catch( InterruptedException | Error any  )  {
                    Thread.interrupted();
                }
                finally {
                    
                }
            }
            //load(_f, args[ 1 ], args[ 2 ], batchSize );
            //System.exit( 0 );
        }
        catch( Exception e )    {
            logger.log(Level.SEVERE, e.toString() );
        }   
    }

    private static Delay timeUnitsFromCmdLine( String _arg )    {
        Iterator< Delay > onKnownDelayOptions = Delay.knownOptions.iterator();
        while( onKnownDelayOptions.hasNext() )  {
            Delay _d = onKnownDelayOptions.next();
            if( _d.name().equals(_arg ) )   {
                return _d;
            }
        }
        return null;
    }
    
    private static void usage() {
    System.out.println( "usage: FileLoader file URL | keyspace | server | batch_size {  number | seconds }" );
    System.out.println( "example: FileLoader /data/bl/ mykeyspace  localhost 10000 10 minutes" );
    }
}
