libname ria "C:\Users\zhenghu\Documents\ADV\sasdata";


%let path=%str(C:\Users\zhenghu\Documents\ADV);
options mprint mlogic symbolgen;

filename dfs "&path./export.tsv" encoding='utf8';
proc import datafile=dfs out=ria.export dbms=dlm replace;delimiter='09'x; GETNAMES=YES;guessingrows=5000;run;

data ria.all; set db_expt1_status_history;run;
data ria.ia060121; set ia060121(rename=(Organization_CRD_=FIRM_CRD_NB));run;
data ria.screen_for_registered; set DB_EXPT1_STATUS_HISTORY_0000;run;
proc freq data=ria.screen_for_registered; table latest_status f3/missing list;run;

proc contents data=ria.ia060121 varnum;run;

proc sql;
select count(*) as cnt, count(distinct FIRM_CRD_NB) as FIRM_CRD_NB from ria.all;
select count(*) as cnt, count(distinct FIRM_CRD_NB) as FIRM_CRD_NB from ria.export;
quit;

proc sort data=ria.all out=all; by FIRM_CRD_NB STTS_EFCTV_DT;run;
proc sort data=all out=all_nodup dupout=all_dup nodupkeys; by FIRM_CRD_NB;run;

data all_dups;
merge all all_dup(keep=FIRM_CRD_NB in=inb);
by FIRM_CRD_NB;
if inb;
run;

proc sort data=ria.ia060121 out=ia060121_nodup dupout=ia060121_dup nodupkeys; by FIRM_CRD_NB;run;

proc sort data=ria.all(where=(STTS_EFCTV_DT<'01jun2021'd )) out=all210601_nodup dupout=all210601_dup nodupkeys; by FIRM_CRD_NB;run;/*# match*/

proc sort data=ria.all(where=(STTS_EFCTV_DT<'01jun2021'd and latest_status in ("120-Day Approval" "Approved"))) out=all210601_nodup dupout=all210601_dup nodupkeys; by FIRM_CRD_NB;run;

data crd_mapping;
merge all210601_nodup(in=ina ) ia060121_nodup(keep=FIRM_CRD_NB in=inb);
by FIRM_CRD_NB;
if ina and inb then flag="both";
else if ina then flag="adv ";
else if inb then flag="site";
run;

proc freq ; table flag/missing list;run;


proc sort data=ria.screen_for_registered(where=(latest_status in ("120-Day Approval" "Approved"))) out=registered_nodup dupout=all210601_dup nodupkeys; by FIRM_CRD_NB;run;

data crd_reg_mapping;
merge registered_nodup(in=ina ) ia060121_nodup(keep=FIRM_CRD_NB in=inb);
by FIRM_CRD_NB;
if ina and inb then flag="both";
else if ina then flag="adv ";
else if inb then flag="site";
run;

proc freq ; table flag/missing list;run;

proc sql;
create table reg_adv_only as 
select * from all where FIRM_CRD_NB in (select distinct FIRM_CRD_NB from crd_reg_mapping where flag="adv ");
create table reg_site_only as 
select * from all where FIRM_CRD_NB in (select distinct FIRM_CRD_NB from crd_reg_mapping where flag="site");
quit;
proc sql;
create table reg_site_only_data as
select * from ria.ia060121 where FIRM_CRD_NB in (select distinct FIRM_CRD_NB from crd_reg_mapping where flag="site");
quit;

proc sql;
select count(*) as cnt, count(distinct FIRM_CRD_NB) as FIRM_CRD_NB from reg_adv_only;
select count(*) as cnt, count(distinct FIRM_CRD_NB) as FIRM_CRD_NB from reg_site_only;
select count(*) as cnt, count(distinct FIRM_CRD_NB) as FIRM_CRD_NB from both_only;
quit;



proc sql;
create table both_only as 
select * from all where FIRM_CRD_NB in (select distinct FIRM_CRD_NB from crd_reg_mapping where flag="both");

select count(*) as cnt, count(distinct FIRM_CRD_NB) as FIRM_CRD_NB from both_only;

create table both_only_more2 as 
select * from both_only a
inner join 
(select distinct FIRM_CRD_NB, count(*) as cnt from both_only group by FIRM_CRD_NB having cnt>1) b
on a.FIRM_CRD_NB=b.FIRM_CRD_NB;
select count(*) as cnt, count(distinct FIRM_CRD_NB) as FIRM_CRD_NB from both_only_more2;
quit;
proc sql;
create table both_only_more2_site as
select * from ria.ia060121 where FIRM_CRD_NB in (select distinct FIRM_CRD_NB from both_only_more2)
order by FIRM_CRD_NB;
quit;

proc sort data=both_only_more2(where=(latest_status in ("120-Day Approval" "Approved"))) out=both_only_more2_approval; by FIRM_CRD_NB descending STTS_EFCTV_DT;run;
proc sort data=both_only_more2_approval out=both_only_more2_approval_nodup dupout=both_only_more2_approval_dup nodupkeys; by FIRM_CRD_NB;run;

data both_only_more2_approval_qa;
merge both_only_more2_approval_nodup(in=ina) both_only_more2_site(in=inb keep=FIRM_CRD_NB SEC_Status_Effective_Date);
by FIRM_CRD_NB;
if ina and inb;
if STTS_EFCTV_DT ne SEC_Status_Effective_Date;
run;

proc sql;
create table both_only_more2_approval_qas as
select a.*, b.SEC_Status_Effective_Date from all a
inner join both_only_more2_approval_qa b
on a.FIRM_CRD_NB=b.FIRM_CRD_NB;
quit;
