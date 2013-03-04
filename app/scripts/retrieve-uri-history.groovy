//Groovy
uri="http://anjackson.net/"
loadProcessor = appCtx.getBean("persistLoadProcessor") //this name depends on config
key = loadProcessor.persistKeyFor(uri)
history = loadProcessor.store.get(key)
history.get(org.archive.modules.recrawl.RecrawlAttributeConstants.A_FETCH_HISTORY).each{historyStr ->
    rawOut.println(historyStr)
}
