clear
cd "C:\Users\tsiqueira4\Downloads\spread-model-master\light_spread-model-master\datos_y_modelos\db\output_vecm_data"
sysuse auto
*This is a comment
//This is a comment

/*
This is a comment
This is a very long comment

Cheetsheet commands:

1) h tsset >> declare data to be time-series data;
2) tsset day >> how to set a attribute where in this example the column is called day;
3) clear >> clear variables
4) webuse invest2 >> download dummy data to allow users to playaround with
5) tsset time company >> used to difine a panel data to companies and time (because both of the variables repeat over time)
6) file >> example datasets.. >> "Example datasets installed with Stata" >> "Use" >> " sp500.dta" >>>>> select another dummy database


How to Estimate Panel Data Regressions:
1) webuse invest2, clear
2) tsset company time, yearly >>> define this is a panel setting
3) reg invest market stock >>> collect panel feature
4) xtreg invest market stock, fe >>> fe = fixed effect model
5) areg invest market stock, absorb(company) >>> for a given company, over time, for an increase market, the invest will change by this number on the coeficient

6) xtreg invest market stock, re >>> re = ramdon effect model




How to Estimate Panel Data Regressions:
1) webuse invest2, clear
2) tsset company time, yearly >>> define this is as panel setting
3) reg invest market stock >>> collect panel feature
4) xtreg invest market stock, fe >>> fe = fixed effect model
5) areg invest market stock, absorb(company) >>> for a given company, over time, for an increase market, the invest will change by this number on the coeficient

6) xtreg invest market stock, re >>> re = ramdon effect model

7) ds >> list all columns of a dataset 
8) di r(varlist)  >> list all columns of a dataset 




*/
clear

use "C:\Users\tsiqueira4\Downloads\spread-model-master\light_spread-model-master\datos_y_modelos\db\output_vecm_data\stata_vecm_data.dta" // set the data base

di r(varlist) // list all columns of a dataset 

// bond_id obs_date month vix fed_funds_rate usd_brl ipca_yoy gdp_yoy sov_spread_5y debt_gdp spread

tsset bond_id obs_date, monthly // define this is as panel setting

reg spread vix fed_funds_rate usd_brl ipca_yoy gdp_yoy sov_spread_5y debt_gdp // collect panel feature
xtreg spread vix fed_funds_rate usd_brl ipca_yoy gdp_yoy sov_spread_5y debt_gdp, fe // fe = fixed effect model
areg spread vix fed_funds_rate usd_brl ipca_yoy gdp_yoy sov_spread_5y debt_gdp, absorb(bond_id)// for a given company, over time, for an increase market, the invest will change by this number on the coeficient


save auto_3, replace

