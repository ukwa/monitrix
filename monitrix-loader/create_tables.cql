CREATE TABLE log(
log_id varchar , 
log_ts varchar PRIMARY KEY, 
status_code varchar, 
downloaded_bytes varchar, 
uri varchar, 
discovery_path varchar, 
referer varchar, 
content_type varchar, 
worker_thread varchar, 
fetch_ts varchar, 
hash varchar, 
annotations varchar, 
ip_address varchar, 
line varchar, 
coarse_ts varchar, 
long_log_ts varchar ) 
WITH COMPACT STORAGE;
  
  
  
create table crawls(
crawl_id varchar PRIMARY KEY,
start_ts varchar, 
end_ts varchar, 
ingested_lines varchar, 
revisit_records varchar  ) 
WITH COMPACT STORAGE;


create table crawl_stats(
stat_ts bigint PRIMARY KEY, 
downloaded_bytes bigint,
uris_crawled bigint, 
new_hosts bigint,
completed_hosts bigint, 
)
WITH COMPACT STORAGE;



create index log_id on log( log_id ) ;
create INDEX coarse_ts on log ( coarse_ts) ;
create index annotations on log ( annotations) ;
create index uri on log( uri );
create index long_log_ts on log( long_log_ts );







