//Groovy
rule = appCtx.getBean("scope").rules.find{ rule ->
  rule.class == org.archive.modules.deciderules.surt.SurtPrefixedDecideRule &&
  rule.decision == org.archive.modules.deciderules.DecideResult.REJECT
}
 
theSurt = "http://(org,northcountrygazette," // ncg is cranky. avoid.
rawOut.print( "result of adding theSurt: ")
rawOut.println( rule.surtPrefixes.considerAsAddDirective(theSurt) )
rawOut.println()
 
//dump the list of surts excluded to check results
rule.surtPrefixes.each{ rawOut.println(it) }

