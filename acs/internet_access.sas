options mprint MLOGIC SYMBOLGEN;

%let path = %str(D:\MPR\NSDUH);
%let myCensusKey = %str(4df39257f305a4e3b810f024e718218e375d9075);
libname acs "&path.\internet";
filename resp temp;

*Read in the list of ACS variables of interet;
*Full ACS variables list is based on 
https://api.census.gov/data/2019/acs/acs5/variables.html
group:B01001/B02001/B06010/B12001/B16001/B19058/B21001/B23001/B23006/C17002;
proc import out= acsvarlst datafile= "&path.\internet_varlst.xlsx" dbms=xlsx replace; 
sheet="internet_varlst"; 
getnames=yes;
run;

*Read in geocodes file (forwarded);
proc import out= geocodes_v2018 datafile= "&path.\all-geocodes-v2018.xlsx" dbms=xlsx replace; 
sheet="v2018geocodes"; 
getnames=yes; 
datarow=6;
run;

data acs.geocodesv2018;
set geocodes_v2018;
rename 'Estimates Geography File: Vintag'n = SummaryLevel	
		b = StateCode
		c = CountyCode
		d = CountySubdivisionCode
		e = PlaceCode
		f = ConsolidtatedCityCode
		g = AreaName;
run;

*States of interest;
data stlst; 
set acs.geocodesv2018(where=(SummaryLevel="040" and areaname not in ("Puerto Rico" "Alaska", "Hawaii")));
run;

*Macro to download ACS varabiles;
%macro ACSdl(StLstDs, acsVarDs, outDsPrelix=Data);
proc sql noprint;
select distinct StateCode into: stfulllst separated by "," from &StLstDs.;
select distinct name into: acsvarfulllst separated by "," from  &acsVarDs.;
quit;
%put &acsvarfulllst;

proc http url="https://api.census.gov/data/2018/acs/acs5?get=&acsvarfulllst&for=tract:*&in=state:&stfulllst&key=&myCensusKey"
  method= "GET"
  out=resp;
run;

libname temp JSON fileref=resp;

data _null_;
  set temp.root (obs=1);
  array elemCols{*} element:;
  length rename $4000;
  do i=1 to dim(elemCols);
    rename=catx(' ',rename, catx('=','element'||compress(put(i,3.)),elemCols{i}));
  end;
  call symputx('rename',rename);
run;

data &outDsPrelix._&acsvards(drop=ordinal_root);
  set temp.root (firstobs=2);
  rename &rename;
run;

libname temp clear;

proc sort data=&outDsPrelix._&acsvards; by state county tract;run;

%mend ACSdl;

*Utility macro to calculate the num of obs of sas dataset;
%macro nobs(ds);
%local nobs dsid rc;
%let dsid=%sysfunc(open(&ds));
%if &dsid EQ 0 %then %do;
  %put ERROR: (nobs) Dataset &ds not opened due to the following reason:;
  %put %sysfunc(sysmsg());
%end;
%else %do;
  %if %sysfunc(attrn(&dsid,WHSTMT)) or
    %sysfunc(attrc(&dsid,MTYPE)) EQ VIEW %then %let nobs=%sysfunc(attrn(&dsid,NLOBSF));
  %else %let nobs=%sysfunc(attrn(&dsid,NOBS));
  %let rc=%sysfunc(close(&dsid));
  %if &nobs LT 0 %then %let nobs=0;
&nobs
%end;
%mend;

*Loop through variable list to download ACS data;
%macro acsVarPullLoop(varLstDs, StLstDs, splitObsNum, outDs);

%local j varNum LoopNum;
%let varNum =%nobs(&varLstDs);
%let LoopNum=%eval(%qsysfunc(ceil(&varNum/&splitObsNum)));
%put &loopNum;

*splitting data;
data 
%let j = 1 ;
%do %while(&j <&LoopNum+1);
	acsvar&j 
    %let j = %eval(&j+1) ;
%end;
;
set &varLstDs;
if idx<=&splitObsNum then output acsvar1;
%let j = 2 ;
%do %while(&j <&LoopNum+1);
	else if idx<=&j*&splitObsNum then output acsvar&j; 
    %let j = %eval(&j+1) ;
%end;
run;

*ACS data download;
%let j = 1 ;
%do %while(&j <&LoopNum+1);
	%ACSdl(&StLstDs, acsvar&j ); 
    %let j = %eval(&j+1) ;
%end;

*merge together downloaded data;
data &outDs;
retain state county tract;
merge 
%let j = 1 ;
%do %while(&j <&LoopNum+1);
data_acsvar&j
    %let j = %eval(&j+1) ;
%end;
;
by state county tract;
run;

%mend;

*50 vars per download loop;
%acsVarPullLoop(acsvarlst,stlst, 50, acs.ACSVARALL);

*Create ACS variable labels;
proc sql noprint;
select catx("=", name, quote(trim(desc))) into :label_list separated by " " from  acsvarlst;
quit;
%put &label_list.;
 
proc datasets library=acs nolist;
modify ACSVARALL;
label &label_list.;
run;
quit;
 
*Read in pdb2018trv4_us to get population related variables;
*https://www2.census.gov/adrm/PDB/2018/docs/pdb2018trv4_us.zip;
proc import out= pdb2018trv4_us datafile= "&path.\pdb2018trv4_us.csv" dbms=csv replace; 
getnames=yes;
run;

data acs.pdb2018trv4_us (drop=_:);
retain state county tract GIDTR;
set pdb2018trv4_us
	(keep=GIDTR State  County  Tract LAND_AREA Tot_Population_CEN_2010 Tot_Population_ACS_12_16
	 rename= (state=_state county=_county tract=_tract GIDTR=_GIDTR));
state = put(_state,z2.);
county = put(_county,z3.);
tract = put(_tract,z6.);
GIDTR = put(_GIDTR,z11.);
run;

proc sort data=acs.pdb2018trv4_us; by state county tract;run;

*Final dataset contain both of ACS variables, summarization & population variables;
data acs.ACSVAR_final;
merge acs.ACSVARALL(in=ina) acs.pdb2018trv4_us;
by  state county tract;
if ina;

run;
