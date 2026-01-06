****************************************************
* Project: Shapiro-Walker-Replication-Stata
* File:    Shapiro-Walker-Replication.do
* Author:  Kavina Peters
* Date:    1/5/26
 *Purpose: creates datasets linking facility ID with latitude longitude coordinates, 
*      which is input to compute facility's spatial intersection with census block groups
*   (Original performed by R code - codeR/spatial_intersection.r, replication done in STATA)
***********************************************************
* Clear environment
clear all
macro drop _all
set more off

* Set version for reproducibility
version 17.0

*set working directory 
cd "\\ict-mc1-fs01.ad.ufl.edu\wvt-ufapps-temp-storage$\UserData\jorggato\Documents\135901-V1"

* Set display options
set linesize 255
set scheme s2color
****************************************************
* Begin geocode prep
****************************************************
*Texas Geocode Prep-------------------------------------------------------------
* Combine facility ID and facility longitude, latitude data from FRS and manually 
*   collected data for facilities in Texas
* --------------------------------------------------------------------------------

import delimited using "TCEQ_coordinates_FRS.csv", clear
preserve

	import delimited "texas_firms_location.csv", clear
    tempfile manual_coords
    save `manual_coords', replace
restore
merge 1:1 facid using `manual_coords', keep(1 3 4 5) update nogen
drop if missing(lat) | missing(lon) //0 observations deleted
export delimited "texas_facility_coordinates.csv", delim(",") replace
*keep(1 3) keeps:
*1: Records that are only in the master dataset (unmatched in using)
*3: Records that are matched between the two datasets.
*update allows Stata to update the values in the master dataset with the values from the using dataset for matched rows.
*nogen prevents Stata from generating the _merge variable, which shows the merge results

use "combined.dta", clear
