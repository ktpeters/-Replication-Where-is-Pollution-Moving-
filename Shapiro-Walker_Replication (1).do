****************************************************
* Project: Shapiro-Walker-Replication-Stata
* File:    Shapiro-Walker-Replication.do
* Author:  Kavina Peters
* Date:    1/5/26
 *Primary Purpose: create datasets linking Texas facility ID with latitude longitude coordinates, which is input to compute facility's spatial intersection with census block groups. (Original performed by R code - codeR/spatial_intersection.r, replication done in STATA)
 
 *Secondary Purpose: Replicate paper visualization of community characteristics in connection with Offset Transactions.

* Paper Conclusions: The paper finds a weak relationship between community characteristics and offset pricing: Finding little evidence that emissions offset transactions disproportionately relocate pollution towards low-income or minority communities. 

*Opportunity for extension: Using U.S. Census Bureau's Pollution Abatement Costs and Expenditures (PACE) 2005 survey data in order to understand interplay between cost of abatement and Offset trade costs. As in Becker (2003)
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

import delimited using "texas/TCEQ_coordinates_FRS.csv", clear
preserve

	import delimited "texas/texas_firms_location.csv", clear
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
*nogen prevents Stata from generating the merge variable, which shows the merge results

=========================================================================================
*** Cleaning and choosing data: Read in Texas generation, use and trade data ***
=========================================================================================
cap erase `temp'
foreach f in "gen" "use" "trade" {

    import delimited using "texas/erc_`f'.csv", delim(",") clear
    keep if status == "approved" & amount > 0
    duplicates drop
    cap confirm var permit_id
    if _rc == 0 {
        destring permit_id, replace
        drop if permit_id == 0
        duplicates tag project_number permit_id pollutant, gen(x)
        assert x == 0
        drop x  
    }
* clean date variables
    ren project_received date
    split(date), parse("/")
    destring date1 date2 date3, replace
    ren date1 date_month
    ren date2 date_day
    ren date3 date_year
    gen year_2digit = date_year < 100 
    replace date_year = 1900 + date_year if (date_year >= 80 & year_2digit == 1)
    replace date_year = 2000 + date_year if (date_year < 80 & year_2digit == 1)
    drop date year_2digit
    
    gen type = "`f'"

    cap confirm file `temp'
    if _rc == 0 append using `temp', force              
    if _rc != 0 tempfile temp 
    sa `temp', replace
}

* separate trade into buy and sell
expand 2 if type == "trade", gen(copied)
replace type = "buy" if type == "trade" & copied == 1
replace type = "sell" if type == "trade" & copied == 0
replace facility_id = buyer_facility_id if type == "buy"
replace permit_id = buyer_permit_id if type == "buy"
replace facility_id = seller_facility_id if type == "sell"
replace permit_id = seller_permit_id if type == "sell"
replace price = subinstr(price, "$", "", .)
replace price = subinstr(price, ",", "", .)
destring price, replace

* drop ununsed columns and reformat
drop status project_submitted project_completed facility_name *retained* buyer_* seller_* firm_* expiration_date use_reason generation_county copied
ren date_year year
ren date_month month
ren date_day day
* drop observations with no facility ID - these are projects (gen/use) at unspecified plants
drop if missing(facility_id)

* generate observation IDs
sort permit_id
gen obs_id = _n
order obs_id


* ============================================================================================ *
*** Spatial average block group characteristics ***
* ============================================================================================ *

* ----------------- merge facility with intersected blockgroups ---------------- *

* import spatial merge file i.e. facility id -> list of intersected blockgroup id
preserve
    import delimited "texas/gisjoin_texas.csv", clear
    ren facid facility_id
    gen year_blkgrp = 2010
    gen fips = 1000 * state_fips + county_fips    
    tempfile spatial_intersection
    sa `spatial_intersection', replace
restore
joinby facility_id using `spatial_intersection'

* compute proportional intersection population as weights : for each intersected blockgroup, calculated % of intersection
gen intersected_area_proportion = intersected_area / blockgroup_area
assert intersected_area_proportion <= 1

* ------------ merge with block group characteristics and aggregate ------------ *

merge m:1 year_blkgrp gisjoin using "texas/neighborhood_char.dta", assert(2 3) keep(3) nogen

* approximate the population in intersected area using intersected area
foreach v of varlist *_pop {
    gen `v'_intersected = `v' * intersected_area_proportion
}

* sum populations over all intersected blockgroup
* these counts as the population (by race) in facility's 1-mi radius
preserve
    collapse (sum) *_pop_intersected, by(obs_id year_blkgrp)
    tempfile pop
    sa `pop', replace
restore

* save permit info before combining
preserve
    keep obs_id project_number permit_id pollutant amount type price year facility_id fips
    * get air district info
    merge m:1 fips using "texas/airDistricts.dta", keep(1 3) keepusing(district) nogen
    drop if district == "" 
    drop fips
    * just keep distinct air districts for fixed effects later
    duplicates drop
    tempfile permit_info
    sa `permit_info' 
restore
* save county info
preserve
    keep obs_id fips intersected_area
    * for each facility, choose the intersected county with the maximum total intersected area
    bys obs_id fips: egen sum_area = sum(intersected_area)
    bys obs_id: egen max_area = max(sum_area)
    keep if round(sum_area, 1) == round(max_area, 1)
    
    keep obs_id fips 
    duplicates drop

    tempfile permit_county
    sa `permit_county', replace
restore


* -------- collapse to compute spatially weighted average characeteristics ---------- *

* estimated intersected total population used as weights for the income from each block group
collapse (mean) median_household_income* per_capita_income* [aw=tot_pop_intersected], by(obs_id year_blkgrp pollutant)
* merge with total hispanic, black, non-hispanic white population estiamted in the facility's 1-mi radius
merge 1:1 obs_id year_blkgrp using `pop', assert(3) keep(3) nogen
ren *_pop_intersected *_pop
* combine with permit characteristics
merge 1:1 obs_id using `permit_info', assert(1 3) keep(3) nogen
* combine with permit county
merge 1:1 obs_id using `permit_county', assert(1 3) keep(3) nogen

* ============================================================================================ *
*** Merge with backlinks to determine original generated permits ***
* ============================================================================================ *

merge m:1 permit_id using "texas/erc_permits_backlinks.dta", keep(1 3) nogen
replace original_permit_id = permit_id if type == "gen"
count if missing(original_permit_id) & type == "use"
assert r(N) == 0

cap erase "texas/erc_permits_backlinks.dta"

* ============================================================================================ *
*** Final formatting ***
* ============================================================================================ *

drop obs_id
gen geo = "texas"