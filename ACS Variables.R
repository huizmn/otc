
library(httr)
library(jsonlite)
library(openxlsx)
library(tidyverse)
library(data.table)

# Location where programs for pulling Census data are stored for 50704 project
Programs_Location <- "d:/MPR/NSDUH/ACS"

# File with list of all states and counties (to be used for looping through states with Tract and/or Block-level as available)
State_County_Xwalk <- "d:/MPR/NSDUH/ACS/all-geocodes-v2018.xlsx"

# Location to print out csv output files
Output_Location <- "d:/MPR/NSDUH/ACS/output"

################  Links to API data ####################

# Your API Key -- Might have to obtain one first and activate it
API_Key <- "4df39257f305a4e3b810f024e718218e375d9075"

#### Base links for the tables needed #####

# Link for all variables on the 2018 ACS 5-year subject tables
# Available geographies: County, ZCTA, Tract
SubjectTables_5yr <- "https://api.census.gov/data/2018/acs/acs5/subject?get=NAME&for=us:*&key=YOUR_KEY_GOES_HERE"

# Link for all variables on the 2018 ACS 5-year Data Profile tables
#Available geographis: County, ZCTA, Tract
DataProfiles_5yr <-  "https://api.census.gov/data/2018/acs/acs5/profile?get=NAME&for=us:*&key=YOUR_KEY_GOES_HERE"

# Line for all variables in the 2018 ACS 5-year Detailed Tables
# Available geographies: County, ZCTA, Tract, Block Group
DetailedTables_5yr <- "https://api.census.gov/data/2018/acs/acs5?get=NAME&for=us:*&key=YOUR_KEY_GOES_HERE"

# Link for all variables on the 2010 Decenniel Census Summary File 1
# Available geographies: County, ZCTA, Tract, Block Group
DecennielSF_2010 <- "https://api.census.gov/data/2010/dec/sf1?get=NAME&for=us:*&key=YOUR_KEY_GOES_HERE"

ACSVarsLst <- c("B01001",
                "B02001",
                "B12001_001E",
                "B12001_002E",
                "B12001_003E",
                "B12001_004E",
                "B12001_009E",
                "B12001_010E",
                "B12001_011E",
                "B12001_012E",
                "B12001_013E",
                "B12001_018E",
                "B12001_019E",
                "B16001",
                "C17002",
                "C17002_001E",
                "C17002_002E",
                "C17002_003E",
                "C17002_004E",
                "C17002_005E",
                "C17002_006E",
                "C17002_007E",
                "C17002_008E",
                "B06010_001E",
                "B06010_002E",
                "B06010_003E",
                "B06010_004E",
                "B06010_005E",
                "B06010_006E",
                "B06010_007E",
                "B06010_008E",
                "B06010_009E",
                "B06010_010E",
                "B06010_011E",
                "B19058_001E",
                "B19058_002E",
                "B19058_003E",
                "B21001_002E",
                "B21001_002E",
                "B21001_003E",
                "B23001",
                "B23006_001E",
                "B23006_002E",
                "B23006_009E",
                "B23006_023E"
)


PullAllVars_byGeo<- function(Year, GeoID, State = "XX", County = "XXX", OutData){
  
  # Make updated API Links
  # We can't pull block level for most tables, just the two outside this code block
  Link_ACSVars <- Update_API_Link(API_Link = DataProfiles_5yr, Year = Year, Variables = ACSVarsLst, GeoID = GeoID, State = State, County = County, Keep_NAME = FALSE)

  #Don't keep name from Decenniel Census, it may not line up with the new tables we are pulling from
  # Couple combos of state/county not available in Decenniel Census
  if((State == "02" & County == "158") | (State == "46" & County == "102")){
    Link_B_Vars <- Update_API_Link(API_Link = DetailedTables_5yr, Year = Year, Variables = B_Vars, GeoID = GeoID, State = State, County = County, Keep_NAME = FALSE)
  } else{
    Link_B_Vars <- Update_API_Link(API_Link = DetailedTables_5yr, Year = Year, Variables = B_Vars, GeoID = GeoID, State = State, County = County, Keep_NAME = FALSE)
    Link_P_Vars <- Update_API_Link(API_Link = DecennielSF_2010, Year = 2010, Variables = P_Vars, GeoID = GeoID, State = State, County = County, Keep_NAME = FALSE)
  }
  
  # Link to get the names (don't match in all datasets, so just get it from the detail tables)
  Link_Names <- Update_API_Link(API_Link = DetailedTables_5yr, Year = Year, Variables = "NAME", GeoID = GeoID, State = State, County = County, Keep_NAME =FALSE)
  
  # Pull Data for updated links
  # We can't pull block level for most tables, just the two outside this code lock
  if(GeoID != "150"){
    GetACS_APIdata(API_Link = Link_DP02Vars, OutDataName = "Dat_DP02Vars")
    GetACS_APIdata(API_Link = Link_DP04Vars, OutDataName = "Dat_DP04Vars")
    GetACS_APIdata(API_Link = Link_S1701Vars, OutDataName = "Dat_S1701Vars")
    GetACS_APIdata(API_Link = Link_S1901Vars, OutDataName = "Dat_S1901Vars")
    GetACS_APIdata(API_Link = Link_S1501Vars_1, OutDataName = "Dat_S1501Vars_1")
    GetACS_APIdata(API_Link = Link_S1501Vars_2, OutDataName = "Dat_S1501Vars_2")
    GetACS_APIdata(API_Link = Link_SOtherVars, OutDataName = "Dat_SOtherVars")
  }
  if((State == "02" & County == "158") | (State == "46" & County == "102")){
    GetACS_APIdata(API_Link = Link_B_Vars, OutDataName = "Dat_B_Vars")
    GetACS_APIdata(API_Link = Link_Names, OutDataName = "Dat_Names")
  } else{
    GetACS_APIdata(API_Link = Link_B_Vars, OutDataName = "Dat_B_Vars")
    GetACS_APIdata(API_Link = Link_P_Vars, OutDataName = "Dat_P_Vars")
    GetACS_APIdata(API_Link = Link_Names, OutDataName = "Dat_Names")
  }
  
  ### Set the by variables for merging based on GeoID
  ####### County #######
  if(GeoID == "050"){byVars <- c("state", "county")} 
  ###### ZCTA #########
  else if(GeoID == "860"){byVars <- c("zip code tabulation area")}
  ####### Census Tract #########
  else if (GeoID == "140"){byVars <- c("state", "county", "tract")}
  ####### Block Group #########
  else if (GeoID == "150"){byVars <- c("state", "county", "tract", "block group")}
  
  
  # Merge datasets together by NAME (after confirming it is unique)
  # Use the ones that are available by block group first
  # For this first merge, keep only the detailed tables observations because those are for the current year
  # the P Vars are from the 2010 Census, so they will likely have different counties
  if((State == "02" & County == "158") | (State == "46" & County == "102")){
    MergedData <- merge(Dat_B_Vars, Dat_Names, by = byVars, all = TRUE)
  } else{
    MergedData <- merge(Dat_B_Vars, Dat_P_Vars, by = byVars, all.x = TRUE)
    MergedData <- merge(Dat_Names, MergedData, by = byVars, all = TRUE)
  }
  
  if (GeoID != "150"){
    MergedData <- merge(MergedData, Dat_DP02Vars, by = byVars, all = TRUE)
    MergedData <- merge(MergedData, Dat_DP04Vars, by = byVars, all = TRUE)
    MergedData <- merge(MergedData, Dat_S1701Vars, by = byVars, all = TRUE)
    MergedData <- merge(MergedData, Dat_S1901Vars, by = byVars, all = TRUE)
    MergedData <- merge(MergedData, Dat_S1501Vars_1, by = byVars, all = TRUE)
    MergedData <- merge(MergedData, Dat_S1501Vars_2, by = byVars, all = TRUE)
    MergedData <- merge(MergedData, Dat_SOtherVars, by = byVars, all = TRUE)
  }
  
  # Output dataset to global environment
  assign(x = OutData, value = MergedData, envir = .GlobalEnv)
  
  # Remove temp links and datasets
  if((State == "02" & County == "158") | (State == "46" & County == "102")){
    remove(Dat_B_Vars, Dat_Names, envir = .GlobalEnv)
  } else {
    remove(Dat_B_Vars, Dat_P_Vars, Dat_Names, envir = .GlobalEnv)
  }
  
  if (GeoID != "150"){
    remove(Dat_DP02Vars, Dat_DP04Vars, Dat_S1701Vars, Dat_S1901Vars, Dat_S1501Vars_1, Dat_S1501Vars_2,  Dat_SOtherVars,
           envir = .GlobalEnv)
    
  }
  
}





NumStates <- length(State_List)

for (i in 1:NumStates){
  
  # Get the current state
  CurrState <- State_List[i]
  
  cat(paste("Pulling data for State: ", CurrState, sep = ""), "\n")
  
  # Pull variables by tract fr the current state (Tract is GeoID "140")
  PullACSAllVars_byGeo(Year = Year, GeoID = "140", State = CurrState, County = "XXX", OutData = "ACS_Tract")
  
  if(i == 1){
    # Write Initial data to csv file -- do not append on first state
    fwrite(x=ACS_Tract, 
           file = CSV_File,
           append = FALSE,
           row.names = FALSE)
    # Initialize data frame to store all Tracts to
    assign(x = "ACS_Tract_All", value = ACS_Tract, envir = .GlobalEnv)
    
  } else {
    # For all other states, append to the csv file
    fwrite(x=ACS_Tract, 
           file = CSV_File,
           append = TRUE,
           row.names = FALSE)
    
    #Append rows to the ACS_Tract_All dataframe
    Append_Tract <- rbind(ACS_Tract_All, ACS_Tract, fill = TRUE)
    assign(x = "ACS_Tract_All", value = Append_Tract, envir = .GlobalEnv)
    
  }
}