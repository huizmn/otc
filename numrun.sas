libname sasout "/app/u07/pqri/2010MeasureTesting/JuneTAP/sas/sasdata/numerator";
libname clms "/app/u07/pqri/2010Measures/JuneTAP";

options compress=yes reuse=yes mlogic mprint macrogen symbolgen mautosource mrecall sasautos=mac obs=max;
      /* mautosource mrecall source notes spool mstored mfile ps=60 ls=140
         mcompilenote=all nosortequals bufno=5 msglevel=i nocenter serror; */

%let startdate = 20100101;
%let enddate   = 20100630;

proc sql noprint;
select distinct meas into: measlst separated by " " from  sasout.numlist;
quit;
%put &measlst;

%macro numrun(list);
%local meas j ;
%let j = 1 ;
%do %while(%qscan(&list,&j,%str( )) ne %str());
    %let meas = %scan(&list,&j) ;
    %put meas=&meas.;

    %include "/app/u07/pqri/2009JohnMeasureTesting/2010/m&meas._qdc.sas";
    %m&meas._qdc;

   proc freq data=sasout.num_&meas;
   table qdc_flag*qdccode1*qdccode2*qdccode3*modifier1*modifier2*modifier3/missing list out=sasout.num_&meas._sum;
   title "&meas. freqs";
   run;

    %put A=&J;
    %let j = %eval(&j+1) ;
    %put B=&J;
%end;
%mend;
%numrun(&measlst);

filename outbox email ("jzheng@ifmc.sdps.org"  "tfoy@ifmc.sdps.org");
data _null_;
     file outbox
          subject="Numerator run done";

	 put "Tom,";
	 put " ";
	 put "2010 JunTAP numerators Run are done, and all the data set are now saved at";
	 put "/app/u07/pqri/2010MeasureTesting/JunTAP/sas/sasdata/numerator";
	 put " ";
	 put "Thanks,";
	 put "John";
     put "Date:  %sysfunc(date(),mmddyy10)";
     put "Time:  %sysfunc(time(),time)";
run;

/*--------------------------------------------------------------------------------------*/

data _null_;
     Date = "%sysfunc(date(),worddate.)";
     Time = "%sysfunc(time(),time.)";

put "===================================================================================";
put "  ENDING JOB INFORMATION:" /;
put "  Job Name: &progname..sas" /;

put "  End Date: " Date ;
put "  End Time: " Time ;
put "===================================================================================";

run;
