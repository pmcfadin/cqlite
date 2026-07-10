#![cfg(all(feature = "state_machine", feature = "cli-helpers", feature = "work-counters", not(feature="tombstones")))]
use std::path::PathBuf;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::sstable::read_work_counters as rwc;
use cqlite_core::Database;
fn root()->Option<PathBuf>{std::env::var("CQLITE_DATASETS_ROOT").ok().map(PathBuf::from).filter(|p|p.exists())}
async fn setup(enabled:bool)->Option<Database>{
  let r=root()?; let schema=r.parent()?.join("schemas").join("time-series.cql");
  let mut cc=cqlite_core::Config::default(); cc.memory.block_cache.enabled=enabled;
  let cfg=IngestionConfig{schema_paths:vec![schema],data_dir:r.join("sstables"),version_hint:Some("5.0".into()),core_config:cc,table_directory_filter:Some("/test_timeseries/".into())};
  Some(ingest(cfg).await.ok()?.database)
}
async fn scan(db:&Database)->usize{let c=StreamingConfig{buffer_size:1,..Default::default()};let mut it=db.execute_streaming("SELECT * FROM test_timeseries.sensor_data",c).await.unwrap();let mut n=0;while let Some(r)=it.next_async().await{r.unwrap();n+=1;}n}
#[tokio::test]
async fn probe(){
  for enabled in [true,false]{
    let Some(db)=setup(enabled).await else{eprintln!("skip");return;};
    scan(&db).await; // warm
    rwc::reset();
    let rows=scan(&db).await;
    eprintln!("cache_enabled={enabled}: rows={rows} reads={} decompress={} allocs={}",rwc::read_calls(),rwc::decompress_calls(),rwc::chunk_path_allocs());
  }
}
