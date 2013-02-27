appCtx.getBean("metadata").keyedProperties.each{ k, v ->
  rawOut.println( k)
  rawOut.println(" $v\n")
}
