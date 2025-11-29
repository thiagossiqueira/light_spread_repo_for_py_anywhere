******************************************************
* Static Fixed-Effects Model for Corporate Bond Spreads
* Author: Thiago Siqueira
* Date: 2025-11-24
******************************************************

*---------------------------------------------------
* 1) Load data
*---------------------------------------------------
global base "C:\Users\tsiqueira4\Downloads\spread-model-master\light_spread-model-master\datos_y_modelos\db\output_panel_data"
cd "$base"
use "panel_data.dta", clear

*---------------------------------------------------
* 2) Encode string IDs and categories
*---------------------------------------------------
capture confirm string variable bond_id
if _rc==0 {
    encode bond_id, gen(bond_id_num)
    drop bond_id
    rename bond_id_num bond_id
}

capture confirm string variable issuer
if _rc==0 {
    encode issuer, gen(issuer_num)
    drop issuer
    rename issuer_num issuer
}

capture confirm string variable bond_type
if _rc==0 encode bond_type, gen(bond_type_cat)
else rename bond_type bond_type_cat

capture confirm string variable sector
if _rc==0 encode sector, gen(sector_cat)
else rename sector sector_cat

*---------------------------------------------------
* 3) Ensure all regressors are numeric
*---------------------------------------------------
destring vix fed_funds_rate usd_brl ipca_yoy gdp_yoy debt_gdp ///
         cf_cash_oper_to_tot_asset amount_issued_to_bs_tot_asset ///
         tot_debt_to_ebitda days_to_maturity synthetic_cds_brl, ///
         replace ignore(",") force

*---------------------------------------------------
* 4) Define the panel
*---------------------------------------------------
xtset bond_id

*---------------------------------------------------
* 5) Fixed-Effects model (robust, clustered by issuer)
*---------------------------------------------------
xtreg spread ///
      vix fed_funds_rate usd_brl ipca_yoy gdp_yoy debt_gdp ///
      cf_cash_oper_to_tot_asset amount_issued_to_bs_tot_asset ///
      tot_debt_to_ebitda days_to_maturity synthetic_cds_brl ///
      i.bond_type_cat i.sector_cat, fe cluster(issuer)
estimates store FE

*---------------------------------------------------
* 6) Random-Effects model (robust, clustered by issuer)
*---------------------------------------------------
xtreg spread ///
      vix fed_funds_rate usd_brl ipca_yoy gdp_yoy debt_gdp ///
      cf_cash_oper_to_tot_asset amount_issued_to_bs_tot_asset ///
      tot_debt_to_ebitda days_to_maturity synthetic_cds_brl ///
      i.bond_type_cat i.sector_cat, re cluster(issuer)
estimates store RE

*---------------------------------------------------
* 7) Classical Hausman test (Stata 15 compatible)
*---------------------------------------------------
xtreg spread ///
      vix fed_funds_rate usd_brl ipca_yoy gdp_yoy debt_gdp ///
      cf_cash_oper_to_tot_asset amount_issued_to_bs_tot_asset ///
      tot_debt_to_ebitda days_to_maturity synthetic_cds_brl ///
      i.bond_type_cat i.sector_cat, fe
estimates store fe_plain

xtreg spread ///
      vix fed_funds_rate usd_brl ipca_yoy gdp_yoy debt_gdp ///
      cf_cash_oper_to_tot_asset amount_issued_to_bs_tot_asset ///
      tot_debt_to_ebitda days_to_maturity synthetic_cds_brl ///
      i.bond_type_cat i.sector_cat, re
estimates store re_plain

hausman fe_plain re_plain, sigmamore

*---------------------------------------------------
* 8) Model Diagnostics (install if missing)
*---------------------------------------------------
capture noisily ssc install xttest3, replace
capture noisily ssc install xtcsd, replace

* Re-run FE model so xttest3 recognizes last estimation
xtreg spread ///
      vix fed_funds_rate usd_brl ipca_yoy gdp_yoy debt_gdp ///
      cf_cash_oper_to_tot_asset amount_issued_to_bs_tot_asset ///
      tot_debt_to_ebitda days_to_maturity synthetic_cds_brl ///
      i.bond_type_cat i.sector_cat, fe

capture noisily xttest3

* Cross-sectional dependence (requires time variable)
* Uncomment and adapt this if you have a time ID:
* xtset bond_id month_id
* capture noisily xtcsd, pesaran abs

******************************************************
display "FE, RE, Hausman, and diagnostic tests completed successfully for panel_data.dta"
******************************************************


*---------------------------------------------------
* 9) Generate the coef chart
*---------------------------------------------------
coefplot , keep(vix fed_funds_rate usd_brl ipca_yoy gdp_yoy debt_gdp ///
    cf_cash_oper_to_tot_asset amount_issued_to_bs_tot_asset ///
    tot_debt_to_ebitda days_to_maturity synthetic_cds_brl) ///
    xline(0) title("Coeficientes normalizados por tipo de factor") ///
	ysize(4) xsize(6) ///
    legend(order(1 "Global" 2 "Macro" 3 "Soberano" 4 "Idiosincrático"))

	
*---------------------------------------------------
* 9) Export Fixed Effects results to Word (.docx)
*---------------------------------------------------
* Install outreg2 if not installed
capture noisily ssc install outreg2, replace

* Re-run FE model (robust, clustered by issuer)
xtreg spread ///
      vix fed_funds_rate usd_brl ipca_yoy gdp_yoy debt_gdp ///
      cf_cash_oper_to_tot_asset amount_issued_to_bs_tot_asset ///
      tot_debt_to_ebitda days_to_maturity synthetic_cds_brl ///
      i.bond_type_cat i.sector_cat, fe cluster(issuer)
