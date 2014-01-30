ENSURE PAUSED
// groovy
count = job.crawlController.frontier.deleteURIs(".*", "^http://de.wikipedia.org/.*")
rawOut.println count + " uris deleted from frontier"

