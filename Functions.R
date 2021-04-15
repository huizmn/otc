#####################################################################
################# User-Defined Functions ############################
#####################################################################

####### Update_API_Link ###########
# Purpose: Updates the API Link to include only needed variables for the query 
# API_Link: The main API call for the table
# Year: The year to pull for the ACS 5-year estimate (i.e. 2016, 2017, or 2018)
# Variables: The list of variables to select
# GeoID: Put the ID of the geographic-level to pull
#        An example of geo-levels available is here:  https://api.census.gov/data/2018/acs/acs5/profile/geography.html
#        Specify either "050" for County, "140" for Census Tract, "860" for ZCTA, or "150" for Block Group
# State:  If GeoID is "860" (Census Tract) or "150" (Block Group), this must be specified. Set to NULL by default
# County: If GeoID is "150" (Block Group), this must be specified.  Set to NULL by default.
# Keep_NAME: TRUE/FALSE Should the NAME variable be kept in the output? Set to FALSE by default
Update_API_Link <- function(API_Link, Year, Variables, GeoID, State = NULL, County = NULL, Keep_NAME = FALSE){
  
  # Collapse the variable list so it can be used in the API call
  Collapsed_Vars <- paste(Variables, collapse = ",")
  
  # Replace the Year of the ACS data
  API_Update <- gsub(pattern = "data/2018",
                     replacement = paste("data/", Year, sep = ""),
                     x = API_Link,
                     fixed = TRUE)
  
  # Replace the one variable NAME with collapsed list of variables, unless Keep_NAME = FALSE
  if (Keep_NAME == TRUE){
    API_Update2 <- gsub(pattern = "get=NAME",
                        replacement = paste("get=NAME,", Collapsed_Vars, sep = ""), 
                        x = API_Update,
                        fixed = TRUE)
  } else{
    API_Update2 <- gsub(pattern = "get=NAME",
                        replacement = paste("get=", Collapsed_Vars, sep = ""), #We don't need the NAME var
                        x = API_Update,
                        fixed = TRUE)
  }
  
  
  # Replace geographical level
  # For Census Tract and Block level, make sure to include State
  # For Block level, make sure to include County
  
  ####### County #######
  if(GeoID == "050"){
    API_Update3 <- gsub(pattern = "for=us:*",
                        replacement = "for=county:*",
                        x = API_Update2,
                        fixed = TRUE)
  } 
  ###### ZCTA #########
  else if(GeoID == "860"){
    API_Update3 <- gsub(pattern = "for=us:*",
                          replacement = "for=zip%20code%20tabulation%20area:*",
                          x = API_Update2,
                          fixed = TRUE)
  }
  ####### Census Tract #########
  else if (GeoID == "140"){
    API_Update3 <- gsub(pattern = "for=us:*",
                        replacement = paste("for=tract:*&in=state:", State, sep = ""),
                        x = API_Update2,
                        fixed = TRUE)
  }
  ####### Block Group #########
  else if (GeoID == "150"){
    API_Update3 <- gsub(pattern = "for=us:*",
                        replacement = paste("for=block%20group:*&in=state:", State,"%20county:", County, sep = ""),
                        x = API_Update2,
                        fixed = TRUE)
  }
  
  # Add the API Key to the end of the string
  API_Update4 <- gsub(pattern = "&key=YOUR_KEY_GOES_HERE",
                      replacement = paste("&key=", API_Key, sep = ""),
                      x = API_Update3,
                      fixed = TRUE)
  
  # Assign to global environment
  assign(x = "API_Link_Updated", value = API_Update4, envir = .GlobalEnv)
}



###### GetACS_APIdata #########
# Purpose: Reads-in API data and stores as usable dataframe in global environment. 
#          Also converts first row of data, which has varnames, to the actual variable names
# API_Link:  The API call for the table 
# OutDataName: A name for the output dataset
GetACS_APIdata <- function(API_Link, OutDataName){
  
  Attempt <- 1 # Initialize to 1
  
  # Try up to 5 times
  # If issues with getting status code != 200, this is usually a very temporary network timeout
  while(Attempt <= 5){
    
    # Get response data from API link
    Response <- GET(API_Link)
    
    #Check the status code, it should be 200
    Status_Code <- Response$status_code
    
    #If not, do not proceed, try again
    if (Status_Code != 200) {
      Attempt <- Attempt + 1
    } 
    # Otherwise, if good, exit the while loop
    else{
      Attempt <- 999999 #Set to a dummy high value so it will exit the loop
    }
    
  }
  
  # If the attempt is 999999, this means all is good
  # Otherwise, there was an issue pulling the data so stop
  if(Attempt != 999999) stop("Status code != 200")
  
  # Convert JSON to usable dataframe
  Data <- fromJSON(rawToChar(Response$content))
  
  # For ACS data, first row should be the column names
  # This converts the first row to variable names
  Data_Names <- Data[1,]
  Data <- as.data.table(Data)
  Data <- Data[-1,] #Remove first row that has the names
  names(Data) <- Data_Names
  
  # Output dataset to global environment
  assign(x = OutDataName, value = Data, envir = .GlobalEnv)
  
}



###### PullAllVars_byGeo #########
# Pull all the variables in the list by specified Geography, combine into one data file
# Year: The year to pull for the ACS 5-year estimate (i.e. 2016, 2017, or 2018)
# GeoID: Put the ID of the geographic-level to pull
#        An example of geo-levels available is here:  https://api.census.gov/data/2018/acs/acs5/profile/geography.html
#        Specify either "050" for County, "140" for Census Tract, "860" for ZCTA, or "150" for Block Group
# State:  If GeoID is "860" (Census Tract) or "150" (Block Group), this must be specified. Set to a dummy value by default
# County: If GeoID is "150" (Block Group), this must be specified.  Set to a dummy value by default.
# OutData: Name for the Output dataset

PullAllVars_byGeo<- function(Year, GeoID, State = "XX", County = "XXX", OutData){
  
  # Make updated API Links
  # We can't pull block level for most tables, just the two outside this code block
  if(GeoID != "150"){
    Link_DP02Vars <- Update_API_Link(API_Link = DataProfiles_5yr, Year = Year, Variables = DP02Vars, GeoID = GeoID, State = State, County = County, Keep_NAME = FALSE)
    Link_DP04Vars <- Update_API_Link(API_Link = DataProfiles_5yr, Year = Year, Variables = DP04Vars, GeoID = GeoID, State = State, County = County, Keep_NAME = FALSE)
    Link_S1701Vars <- Update_API_Link(API_Link = SubjectTables_5yr, Year = Year, Variables = S1701Vars, GeoID = GeoID, State = State, County = County, Keep_NAME = FALSE)
    Link_S1901Vars <- Update_API_Link(API_Link = SubjectTables_5yr, Year = Year, Variables = S1901Vars, GeoID = GeoID, State = State, County = County, Keep_NAME = FALSE)
    Link_S1501Vars_1 <- Update_API_Link(API_Link = SubjectTables_5yr, Year = Year, Variables = S1501Vars_1, GeoID = GeoID, State = State, County = County, Keep_NAME = FALSE)
    Link_S1501Vars_2 <- Update_API_Link(API_Link = SubjectTables_5yr, Year = Year, Variables = S1501Vars_2, GeoID = GeoID, State = State, County = County, Keep_NAME = FALSE)  
    Link_SOtherVars <- Update_API_Link(API_Link = SubjectTables_5yr, Year = Year, Variables = SOtherVars, GeoID = GeoID, State = State, County = County, Keep_NAME = FALSE)
   }

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



###### PullAllVars_Tract #########
# Loops through all states to pull Census Tract variables, will append directly to a csv file
# Year: The year to pull for the ACS 5-year estimate (i.e. 2016, 2017, or 2018)
# CSV_File: Full path for the csv file to write to (loops through each state then appends the csv file)
PullAllVars_Tract <- function(Year, CSV_File){
  
  NumStates <- length(State_List)
  
  for (i in 1:NumStates){
    
    # Get the current state
    CurrState <- State_List[i]
    
    cat(paste("Pulling data for State: ", CurrState, sep = ""), "\n")
    
    # Pull variables by tract fr the current state (Tract is GeoID "140")
    PullAllVars_byGeo(Year = Year, GeoID = "140", State = CurrState, County = "XXX", OutData = "ACS_Tract")
    
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
}


###### PullChunks_BlockGroup #########
# Purpose: Read-in all API data at block-group level for all FIPS codes in chunks
# Year:  The year to pull for the ACS 5-year estimate (i.e. 2016, 2017, or 2018)
# Start:  The starting number for chunking
# End: The ending number for chunking
# CSV_File: Full path for the csv file to write to (loops through each state then appends the csv file)
PullChunks_BlockGroup <- function(Year, Start, End, CSV_File){
  
  # This will append to a log file for the Year
  LogFileName <- paste(Programs_Location,"Block Group-level Data Pull Log.txt", sep = "/")
  
  sink(LogFileName , append = TRUE)
  
  # Put the start time
  StartTime <- Sys.time()
  cat(paste("Starting pull for FIPS", Start, "to", End, "at", StartTime, sep = " "), "\n")
  
  # Loop through the list for the start and end of this chunk
  for (i in Start:End){
    
    CurrState <- County_State_List[i]
    CurrCounty <- County_List[i]
    
    cat(paste("i =", i, "Pulling data for State:", CurrState, ", County:", CurrCounty, sep = " "), "\n")
    
    PullAllVars_byGeo(Year = Year, GeoID = "150", State = CurrState, County = CurrCounty, OutData = "ACS_Block")
    
    # Append this data to the output csv file
    if(i == 1){
      # Write Initial data to csv file -- do not append on first state
      fwrite(x=ACS_Block, 
             file = CSV_File,
             append = FALSE,
             row.names = FALSE)
      
      # Initialize data frame to store all Block Groups to
      assign(x = "ACS_Block_All", value = ACS_Block, envir = .GlobalEnv)
      
    } else {
      # For all other states, append to the csv file
      fwrite(x=ACS_Block, 
             file = CSV_File,
             append = TRUE,
             row.names = FALSE)
      
      #Append rows to the ACS_Block_All dataframe
      Append_Tract <- rbind(ACS_Block_All, ACS_Block, fill = TRUE)
      assign(x = "ACS_Block_All", value = Append_Tract, envir = .GlobalEnv)
    }
    
  }
  
  # Log the End time and total time this took
  EndTime <- Sys.time()
  cat(paste("Finished pull for FIPS", Start, "to", End, "at", EndTime, sep = " "), "\n")
  cat("Total Time to run Chunk:", "\n\n")
  cat(EndTime - StartTime)
  
  sink()
  
}







