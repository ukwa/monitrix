// groovy
// see org.archive.crawler.frontier.BdbMultipleWorkQueues.forAllPendingDo()
 
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import java.util.regex.Pattern;
 
MAX_URLS_TO_LIST = 1000
m = null
verbose = true
pattern = Pattern.compile(".*\\.com.*")
 
pendingUris = job.crawlController.frontier.pendingUris
 
rawOut.println "(this seems to be more of a ceiling) pendingUris.pendingUrisDB.count()=" + pendingUris.pendingUrisDB.count()
rawOut.println()
 
cursor = pendingUris.pendingUrisDB.openCursor(null, null);
if (m == null) {
    key = new DatabaseEntry();
} else {
    byte[] marker = m.getBytes();
    key = new DatabaseEntry(marker);
}
value = new DatabaseEntry();
count = 0;

// For cursor work:
// cursor.getSearchKey(key, value, null);

ArrayList<String> results = new ArrayList<String>(MAX_URLS_TO_LIST);
 
while (cursor.getNext(key, value, null) == OperationStatus.SUCCESS && count < MAX_URLS_TO_LIST) {
    if (value.getData().length == 0) {
        continue;
    }
    curi = pendingUris.crawlUriBinding.entryToObject(value);
    if(pattern.matcher(curi.toString()).matches()) {
                        if (verbose) {
                            results.add("[" + curi.getClassKey() + "] " 
                                    + curi.shortReportLine());
                        } else {
                            results.add(curi.toString());
                        }
    rawOut.println curi
    count++
    }
}

cursor.close(); 
 
rawOut.println()
rawOut.println count + " pending urls listed"

