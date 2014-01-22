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
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    
    
    @SuppressWarnings("unchecked")
    public final static void load( final File _file, final String _keySpace, final String _host, final int _batchSize ) throws Exception {

        keySpace = AstyanaxInitializer.doInit( "Test Cluster", _host, _keySpace );
        final MutationBatch batch = keySpace.prepareMutationBatch();
        
        System.out.println( "got keyspace " + keySpace.getKeyspaceName() + " from Astyanax initializer" );
        
        final LineIterator onLines = FileUtils.lineIterator( _file );
        
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
        
        while( onLines.hasNext() ){
            final long _seq = rb.next();
            final SingleLogLineAsString event = rb.get( _seq );
            event.setValue( onLines.nextLine() );
            rb.publish(_seq);
            _lineCount++;
        }

        
        disruptor.shutdown();
        _lapse = System.nanoTime() - _start;

        System.out.println( "handled " + _lineCount + " log lines in " + _lapse + " nanoseconds" );
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
        if( ! ( args.length == 4 ) ) {
            usage();
            System.exit( 1 );
        }
        try {
            final File _f = new File( args[ 0 ] );
            final int batchSize = Integer.parseInt( args[ 3 ] );
            load(_f, args[ 1 ], args[ 2 ], batchSize );
            System.exit( 0 );
        }
        catch( Exception e )    {
            logger.log(Level.SEVERE, e.toString() );
        }   
    }

    
    private static void usage() {
    System.out.println( "usage: FileLoader { file | keyspace | server | batch_size }" );
    }
}
