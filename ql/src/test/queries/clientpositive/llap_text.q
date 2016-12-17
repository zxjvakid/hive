set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.fetch.task.conversion=none;

SET hive.llap.io.enabled=false;
set hive.llap.cache.allow.synthetic.fileid=true;


DROP TABLE text_llap;

CREATE TABLE text_llap(
    ctinyint TINYINT,
    csmallint SMALLINT,
    cint INT,
    cbigint BIGINT,
    cfloat FLOAT,
    cdouble DOUBLE,
    cstring1 STRING,
    cstring2 STRING,
    ctimestamp1 TIMESTAMP,
    ctimestamp2 TIMESTAMP,
    cboolean1 BOOLEAN,
    cboolean2 BOOLEAN)
row format serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
 stored as inputformat "org.apache.hadoop.hive.llap.io.decode.LlapTextInputFormat" 
 outputformat "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";

alter table text_llap set serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';


insert into table text_llap
select ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 from alltypesorc 
where cboolean2 is not null or cstring1 is not null or ctinyint is not null;

create table text_llap1 like text_llap; 
create table text_llap100 like text_llap; 
create table text_llap1000 like text_llap; 

insert into table text_llap1
select ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 from alltypesorc 
where cboolean2 is not null or cstring1 is not null or ctinyint is not null limit 1;

insert into table text_llap100
select ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 from alltypesorc 
where cboolean2 is not null or cstring1 is not null or ctinyint is not null limit 100;

insert into table text_llap1000
select ctinyint, csmallint, cint, cbigint, cfloat, cdouble, cstring1, cstring2, ctimestamp1, ctimestamp2, cboolean1, cboolean2 from alltypesorc 
where cboolean2 is not null or cstring1 is not null or ctinyint is not null limit 1000;


SET hive.llap.io.enabled=true;
SET hive.vectorized.execution.enabled=true;
set hive.llap.io.encode.slice.row.count=90;

select ctinyint, cstring1, cboolean2 from text_llap100 order by ctinyint, cstring1, cboolean2;
select * from text_llap100 order by cint, cstring1, cstring2;
select csmallint, cstring1, cboolean2 from text_llap100 order by csmallint, cstring1, cboolean2;


DROP TABLE text_llap;
