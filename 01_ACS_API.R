#########################################################################
#####  Program: 01_ACS_API.R
#####  Programmer: Brittany Colip
#####  Date: 2020-10-30
#####  Purpose:  Pull publicly available American Community Survey 
#####            from Census.gov API
#####  Notes:  
######       - If needed, API Key as of 2020-06-18 = 4df39257f305a4e3b810f024e718218e375d9075
#####        - R Tutorial for accessing API:  https://www.dataquest.io/blog/r-api-tutorial/
#####        - More information about how to use Census APIs found here:  https://www.census.gov/content/dam/Census/data/developers/api-user-guide/api-guide.pdf
#####        - Info about all tables can be found here:  https://api.census.gov/data.html
##########################################################################

# Load necessary libraries
# If needed, install first (uncomment next line)

#install.packages(c("httr", "jsonlite"))

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



############### Variables to keep in each table ##################
# Read in the VarLists.R program
source(paste(Programs_Location, "VarLists.R", sep = "/"))

################ Read-in User Defined Functions ##################
source(paste(Programs_Location, "Functions.R", sep = "/"))

##################################################################
################ Load and Prep FIPS List #########################
##################################################################

# The data starts in the fifth row
FIPS <- read.xlsx(State_County_Xwalk, sheet = 1, startRow = 5)

# We need to remove the County Codes that are "000"
# This reduces the size from ~48k to ~24k observations
FIPS <- as.data.table(FIPS[FIPS$`County.Code.(FIPS)` != "000",])

# We need to remove the duplicates by County and State
# This file had some dup County/State because of city and subdivision codes
KeyCols <- c("State.Code.(FIPS)", "County.Code.(FIPS)")
FIPS <- unique(FIPS, by = KeyCols)

# Put together a State list and a County List
State_List <- unique(FIPS$`State.Code.(FIPS)`) # For looping through States for Census-tract level geography
County_State_List <- FIPS$`State.Code.(FIPS)` #For looping through counties for block-level geography
County_List <- FIPS$`County.Code.(FIPS)` #For looping through counties for block-level geography

State_List

################################################################
################  Get ACS Data and Prep #######################
###############################################################

# Write to csv for final output
# Save to R Dataset for later cleaning

# County-level
PullAllVars_byGeo(Year = 2018, GeoID = "050", State = "XX", County = "XXX", OutData = "ACS_County")
write.csv(ACS_County, paste(Output_Location, "County", "Y2018_ACS_Vars_County.csv", sep = "/"), row.names = FALSE, na = "")
save(ACS_County, file = paste(Output_Location, "County", "ACS_County.rda", sep = "/"))

# ZCTA-level
PullAllVars_byGeo(Year = 2018, GeoID = "860", State = "XX", County = "XXX", OutData = "ACS_ZCTA")
write.csv(ACS_ZCTA, paste(Output_Location, "ZCTA","Y2018_ACS_Vars_ZCTA.csv", sep = "/"), row.names = FALSE, na = "")
save(ACS_ZCTA, file = paste(Output_Location, "ZCTA", "ACS_ZCTA.rda", sep = "/"))

# Census Tract-level
PullAllVars_Tract(Year = 2018, CSV_File = paste(Output_Location, "Census Tract", "Y2018_ACS_Vars_Tract.csv", sep = "/"))
save(ACS_Tract_All, file = paste(Output_Location, "Census Tract", "ACS_Tract_All.rda", sep = "/"))

### Before running blocks, clean up the global environment
remove(ACS_County, ACS_Tract, ACS_Tract_All, ACS_ZCTA)



PullAllVars_byGeo(Year = 2018, GeoID = "140", State = "01", County = "XXX", OutData = "ACS_Tract")
write.csv(ACS_Tract, paste(Output_Location, "ZCTA","Y2018_ACS_Vars_ZCTA01.csv", sep = "/"), row.names = FALSE, na = "")






