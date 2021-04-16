%let path = %str(D:\MPR\NSDUH\);
%let myCensusKey = %str(4df39257f305a4e3b810f024e718218e375d9075);
libname acs "&path.sasdata";
filename resp temp;

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

*read in geocodes file;
proc import out= geocodes_v2018 datafile= "&path.all-geocodes-v2018.xlsx" dbms=xlsx replace; 
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

*create states of interest;
data stlst; 
set acs.geocodesv2018(where=(SummaryLevel="040" and areaname not in ("Puerto Rico" "Alaska", "Hawaii")));
run;

*read in the ACS variable list;
proc import out= acsvarlst datafile= "&path.acsVarFullLst.xlsx" dbms=xlsx replace; 
sheet="fulllst"; 
getnames=yes;
run;

*macro to download ACS varabiles;
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

*loop through variable list to download ACS data;
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
	else if idx<=&j*&splitobs then output acsvar&j; 
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

*50 vars per loop;
%acsVarPullLoop(acsvarlst,50, acs.ACSVARALL);

*create labels;
proc sql noprint;
select catx("=", name, quote(trim(desc))) into :label_list separated by " " from  acsvarlst;
quit;
%put &label_list.;
 
proc datasets library=acs nolist;
modify ACSVARALL;
label &label_list.;
run;
quit;
 
*read in pdb2018trv4_us;
proc import out= pdb2018trv4_us datafile= "&path.pdb2018trv4_us.csv" dbms=csv replace; 
getnames=yes;
run;

data acs.pdb2018trv4_us (drop=_:);
retain state county tract GIDTR;
set pdb2018trv4_us
	(keep=GIDTR State State_name County County_name Tract LAND_AREA Tot_Population_CEN_2010 Tot_Population_ACS_12_16
	 rename= (state=_state county=_county tract=_tract GIDTR=_GIDTR));
state = put(_state,z2.);
county = put(_county,z3.);
tract = put(_tract,z6.);
GIDTR = put(_GIDTR,z11.);
run;

proc sort data=acs.pdb2018trv4_us; by state county tract;run;

data acs.ACSVAR_final;
merge acs.ACSVARALL(in=ina) acs.pdb2018trv4_us;
by  state county tract;
if ina;

B12001_never_married = sum(B12001_003E + B12001_012E);
B12001_now_married = sum(B12001_004E + B12001_013E);
B12001_widowed = sum(B12001_009E + B12001_018E);
B12001_divorced = sum(B12001_010E + B12001_019E);

B23001_Armed_Forces = sum(B23001_005E ,B23001_012E ,B23001_019E ,B23001_026E ,B23001_033E ,B23001_040E ,B23001_047E ,B23001_054E ,B23001_061E 
,B23001_068E ,B23001_091E ,B23001_098E ,B23001_105E ,B23001_112E ,B23001_119E ,B23001_126E ,B23001_133E ,B23001_140E ,B23001_147E ,B23001_154E);

B23001_civilian_employed =sum(B23001_007E ,B23001_014E ,B23001_021E ,B23001_028E ,B23001_035E ,B23001_042E ,B23001_049E ,B23001_056E ,B23001_063E ,
B23001_070E ,B23001_093E ,B23001_100E ,B23001_107E ,B23001_114E ,B23001_121E ,B23001_128E ,B23001_135E ,B23001_142E ,B23001_149E ,B23001_156E);

B23001_civilian_unemployed =sum(B23001_008E ,B23001_015E ,B23001_022E ,B23001_029E ,B23001_036E ,B23001_043E ,B23001_050E ,B23001_057E ,B23001_064E ,
B23001_071E ,B23001_094E ,B23001_101E ,B23001_108E ,B23001_115E ,B23001_122E ,B23001_129E ,B23001_136E ,B23001_143E ,B23001_150E ,B23001_157E	);

B23001_not_in_labor_force =sum(B23001_009E ,B23001_016E ,B23001_023E ,B23001_030E ,B23001_037E ,B23001_044E ,B23001_051E ,B23001_058E ,B23001_065E ,
B23001_072E ,B23001_077E ,B23001_082E ,B23001_087E ,B23001_095E ,B23001_102E ,B23001_109E ,B23001_116E ,B23001_123E ,B23001_130E ,B23001_137E ,B23001_144E ,
B23001_151E ,B23001_158E ,B23001_163E ,B23001_168E ,B23001_173E	);
run;
