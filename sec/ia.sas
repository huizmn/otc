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











options mprint mlogic symbolgen validvarname=V7;;

%let cutOffDate=%str('31May2021'd);
%let cutOffDateStart=%str('31Jan2018'd);
proc format;
invalue stts_lvl
'Approved'= 5
'120-Day Approval'= 5
'Holding'= 0
'Pending'= 0
'Postponed'= 0
'Terminated'= 2
'Voluntary Withdrawal'= 2
'Cancelled'= 2
'Revoked'= 2
'Abandoned'= 2
'Denied'= 2
'Transitioned to Relying Adviser'= 2
'Suspended'= 2
'Withdrawn'= 2
'Voluntary Postponement'= 2
other = -1;
run;

/*
# inputDf must have RGSTN_STTS_DS, FIRM_CRD_NB (CRD number) and STTS_EFCTV_DT from the oracle database
# RGSTN_STTS_DS is status description, status code should work too
# FIRM_CRD_NB is firm CRD number. Other unique identifier should work too
# STTS_EFCTV_DT should be a date so the pd.to_datetime(db_data1.STTS_EFCTV_DT) step is done before this
*/


filename dfs "&path./export.tsv" encoding='utf8';
filename ia2106 "&path./ia060121.csv";

proc import datafile=dfs out=ria.export dbms=dlm replace;delimiter='09'x; GETNAMES=YES;guessingrows=5000;run;
proc import datafile=ia2106 out=ria.ia060121 dbms=csv replace;GETNAMES=YES;guessingrows=5000;run;

/*
# 1. Add a 'status_level' column. This will order the status of the same day
# see dct_stts_lvl and the next code as an example, 
# changing the values will change the order. 
# e.g. if cancelled is 2 and approved is 5, then when both occurs on the same day, the final status is approved
*/

data ria.export;
retain FIRM_CRD_NB STTS_EFCTV_DT RGSTN_STTS_DS;
set ria.export;
status_level=input(RGSTN_STTS_DS, stts_lvl.);
run;

/*# 2. sort inputDf by FIRM_CRD_NB, STTS_EFCTV_DT (oldest to newest), status_level (ascending)*/

proc sort data= ria.export(where= (STTS_EFCTV_DT <= &cutOffDate )) out=export; by FIRM_CRD_NB descending STTS_EFCTV_DT descending status_level;run;
/*
# 3. add a 'temp_status' column, 
# most important is to change pending/holding to previous status. 
# e.g. if status is denied, pending, pending, approved, pending, holding, terminated
# it should become denied, denied, denied, approved, approved, apporoved, teminated.
# it can be grouped to some degree if wanted, but are not required, 
# e.g. terminated, withdraw etc can be called non-registered, approve/approve120 can be registered  

# return the temp_status column to be added to the original data, 
# or return a data with temp_status column added
*/

proc sort data=export out=export_latestStat nodupkeys; by FIRM_CRD_NB;run;


/*
# 1. keep data of  STTS_EFCTV_DT <= cutOffDate
STTS_EFCTV_DT between &cutOffDate and &cutOffDateStart
# 2. keep status with latest STTS_EFCTV_DT date. 
# 3. generate a registered (True/False) column if needed,
# if temp_status is either approved or approved 120 day, it is registered, otherwise, not registered.
# 4. get a count of unique  FIRM_CRD_NB of regiestered=True
# or get counts of both True and False
*/

proc sql;
create table exportf as
select a.*, b.STTS_EFCTV_DT as STTS_EFCTV_DT_latest , b.RGSTN_STTS_DS as stts_latest
, case when (a.STTS_EFCTV_DT=b.STTS_EFCTV_DT and a.RGSTN_STTS_DS=b.RGSTN_STTS_DS) then 1 else 0 end as flag_latest
, case when (a.STTS_EFCTV_DT=b.STTS_EFCTV_DT and a.RGSTN_STTS_DS=b.RGSTN_STTS_DS) and a.status_level=5 then 1 else 0 end as flag_registered
from export a
left join export_latestStat b
on a.FIRM_CRD_NB=b.FIRM_CRD_NB
order by FIRM_CRD_NB, STTS_EFCTV_DT, status_level;
quit;

proc freq data=export_latestStat;table status_level/missing list;run;
proc freq data=exportf;
table flag_latest* flag_registered/missing list;
run;

proc sort data=ria.ia060121(keep=Organization_CRD_ rename=(Organization_CRD_=FIRM_CRD_NB)) out=ia060121;by FIRM_CRD_NB;run;

data comparsion;
merge export_latestStat(in=ina where=(status_level=5 )) ia060121(in=inb);
by FIRM_CRD_NB;
length flag_source $20.;
if ina and inb then flag_source="both";
else if ina then flag_source="ADV registered";
else if inb then flag_source="SEC IA list";
run;
proc freq data=comparsion;
table flag_source/missing list;
run;


data export_count;
set export;
where 
/*# 1. keep data of  STTS_EFCTV_DT <= cutOffDate*/
/*STTS_EFCTV_DT between &cutOffDate and &cutOffDateStart*/
STTS_EFCTV_DT <= &cutOffDate 
/*# 2. keep status with latest STTS_EFCTV_DT date. */
;
/*
# 3. generate a registered (True/False) column if needed,
# if temp_status is either approved or approved 120 day, it is registered, otherwise, not registered.
*/

if STTS_EFCTV_DT=STTS_EFCTV_DT_latest then do
	elig=1;
	if RGSTN_STTS_DS=stts_latest and status_level=5 then regiestered=1;
	else regiestered=0;
end;
run;
proc freq data=export_count;
table elig*regiestered/missing list;
run;



/*
# 4. get a count of unique  FIRM_CRD_NB of regiestered=True
# or get counts of both True and False
*/


proc freq data=ria.export;table RGSTN_STTS_DS/missing list;run;
proc freq data=export;table RGSTN_STTS_DS*ds/missing list;run;

proc freq data=ria.export;table RGSTN_STTS_DS/missing list; format RGSTN_STTS_DS $sttslv.;run;

