

package at.ac.ait.ubicity.fileloader.util;

import java.io.File;
import java.io.FilenameFilter;

/**
 *
 * @author jan van oort
 */
public final class LogFileNameFilter implements FilenameFilter {

    public final static String FILE_NAME_SUFFIX = ".log";
    
    @Override
    public boolean accept(File dir, String name) {
        return name.endsWith( FILE_NAME_SUFFIX );
    }

}
