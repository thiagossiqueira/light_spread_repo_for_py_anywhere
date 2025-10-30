version 15.0
capture log close
set more off

* ========= Caminhos (AJUSTE SE PRECISAR) =========
local path_base "C:\Users\tsiqueira4\Downloads\spread-model-master\light_spread-model-master\datos_y_modelos\db\output_vecm_data\stata_vecm_data.dta"
local path_out  "C:\Users\tsiqueira4\Downloads\spread-model-master\light_spread-model-master\datos_y_modelos\db\output_vecm_data\stata_vecm_data_clean.dta"
local path_fer  "C:\Users\tsiqueira4\Downloads\spread-model-master\light_spread-model-master\datos_y_modelos\db\brazil_domestic_corp_bonds\feriados_nacionais.xls"

* ========= 0) Abrir base =========
use "`path_base'", clear
ds
di as txt ">> Variáveis na base: `r(varlist)'"


********************************************************************************
* 1) Aliases essenciais (case-insensitive)
********************************************************************************
local need_vars bond_id obs_date spread vix fed_funds_rate usd_brl ipca_yoy gdp_yoy sov_spread_5y debt_gdp
local alt_bond_id        "bondid"
local alt_obs_date       "obsdate"
local alt_fed_funds_rate "fedfunds fed_funds fedfunds_rate fedrate"
local alt_sov_spread_5y  "sov_spread5y sovspread5y sovspread_5y cds5y cds_5y"
local alt_debt_gdp       "debtgdp"

program drop _all
program define __alias_or_error
    syntax , target(string)

    capture confirm variable `target'
    if !_rc exit

    unab _allvars : *
    local found ""
    foreach w of local _allvars {
        if lower("`w'")==lower("`target'") local found "`w'"
    }
    if "`found'"!="" {
        capture confirm variable `target'
        if _rc clonevar `target' = `found'
        exit
    }

    local alts : local alt_`target'
    if "`alts'"!="" {
        foreach a of local alts {
            unab _allvars : *
            foreach w of local _allvars {
                if lower("`w'")==lower("`a'") local found "`w'"
            }
            if "`found'"!="" {
                capture confirm variable `target'
                if _rc clonevar `target' = `found'
                exit
            }
        }
    }

    local target_nound = subinstr("`target'","_","",.)
    unab _allvars : *
    foreach w of local _allvars {
        if lower(subinstr("`w'","_","",.))==lower("`target_nound'") local found "`w'"
    }
    if "`found'"!="" {
        capture confirm variable `target'
        if _rc clonevar `target' = `found'
        exit
    }

    di as error "!! Variável ausente: `target'"
end

local missing ""
foreach v of local need_vars {
    quietly __alias_or_error, target(`v')
    capture confirm variable `v'
    if _rc local missing "`missing' `v'"
}
if "`missing'"!="" {
    di as error "!! Faltam variáveis essenciais:`missing'"
    exit 198
}


********************************************************************************
* 2) Strings -> Num (ponto como decimal)
********************************************************************************
local must_numeric spread bs_tot_asset cf_cash_from_oper tot_debt_to_ebitda amount_issued

* Aliases comuns (amountissued/exchangeid)
capture confirm variable amount_issued
if _rc {
    capture confirm variable amountissued
    if !_rc clonevar amount_issued = amountissued
}
capture confirm variable exchange_id
if _rc {
    capture confirm variable exchangeid
    if !_rc clonevar exchange_id = exchangeid
}

local nbsp = char(160)

foreach v of local must_numeric {
    capture confirm variable `v'
    if !_rc {
        capture confirm string variable `v'
        if !_rc {
            di as txt ">> Convertendo string -> numeric: `v'"
            gen strL `v'_str = `v'
            replace `v'_str = "-" + substr(`v'_str, 2, length(`v'_str)-2) ///
                if substr(`v'_str,1,1)=="(" & substr(`v'_str,-1,1)==")"
            replace `v'_str = subinstr(`v'_str, ",","",.)
            replace `v'_str = subinstr(`v'_str, " ","",.)
            replace `v'_str = subinstr(`v'_str, "R$","",.)
            replace `v'_str = subinstr(`v'_str, "US$","",.)
            replace `v'_str = subinstr(`v'_str, "$","",.)
            replace `v'_str = subinstr(`v'_str, "%","",.)
            replace `v'_str = subinstr(`v'_str, "`nbsp'","",.)
            destring `v'_str, force generate(`v'_num)
            drop `v'_str
            drop `v'
            rename `v'_num `v'
        }
    }
}


********************************************************************************
* 3) Categóricas -> IDs
********************************************************************************
local catvars issuer bond_type sector exchange_id
foreach c of local catvars {
    local found ""
    unab ALLVARS : *
    foreach w of local ALLVARS {
        if lower("`w'")==lower("`c'") local found "`w'"
    }
    if "`found'"!="" {
        capture confirm string variable `found'
        if !_rc {
            di as txt ">> Codificando categórica: `found' → `c'_id"
            replace `found' = stritrim(`found')
            replace `found' = strtrim(`found')
            replace `found' = upper(`found')
            encode `found', gen(`c'_id)
        }
        else gen long `c'_id = `found'
    }
}


********************************************************************************
* 4) Painel mensal
********************************************************************************
capture confirm numeric variable obs_date
if _rc {
    gen double __date_try = date(obs_date, "YMD")
    replace __date_try = date(obs_date, "DMY") if missing(__date_try)
    replace __date_try = date(obs_date, "MDY") if missing(__date_try)
    format __date_try %td
    gen obs_m = mofd(__date_try)
    drop __date_try
}
else gen obs_m = mofd(obs_date)
format obs_m %tm

capture confirm numeric variable bond_id
if _rc encode bond_id, gen(id)
else    gen long id = bond_id

gen double obs_d_start = dofm(obs_m)           // 1º dia do mês
gen double obs_d_eom   = dofm(obs_m + 1) - 1   // fim do mês (calendário)
format obs_d_start obs_d_eom %td

xtset id obs_m


********************************************************************************
* 5) Maturity & Tenores
********************************************************************************
local matvar ""
unab ALLV : *
foreach w of local ALLV {
    if lower("`w'")=="maturity" local matvar "`w'"
}
if "`matvar'"!="" {
    capture confirm numeric variable `matvar'
    if _rc {
        gen double maturity_d = date(`matvar', "YMD")
        replace maturity_d = date(`matvar', "DMY") if missing(maturity_d)
        replace maturity_d = date(`matvar', "MDY") if missing(maturity_d)
    }
    else gen double maturity_d = `matvar'
    format maturity_d %td
}
else {
    di as error "!! 'maturity' ausente"
    exit 198
}

* Tenor calendário (robustez)
gen double tenor_years_cal_eom   = (maturity_d - obs_d_eom)   / 365.25
gen double tenor_years_cal_start = (maturity_d - obs_d_start) / 365.25
label var tenor_years_cal_eom   "Tenor (anos) - calendário, EoM"
label var tenor_years_cal_start "Tenor (anos) - calendário, início do mês"

* --------- 5B) ACT/252 com feriados ---------
tempfile feriados
preserve
    import excel "`path_fer'", firstrow clear
    local coldate ""
    unab _all : *
    foreach c of local _all {
        if lower("`c'")=="data" local coldate "`c'"
    }
    if "`coldate'"=="" {
        ds
        local coldate : word 1 of `r(varlist)'
    }
    gen double d = date(`coldate', "MDY")   // ajuste p/ DMY se necessário
    format d %td
    keep d
    drop if missing(d)
    duplicates drop
    sort d
    gen byte holiday_flag = 1
    save "`feriados'", replace
restore

preserve
    quietly summarize obs_m, meanonly
    local mmin = r(min)
    local mmax = r(max)
    gen double __start = dofm(`mmin')
    gen double __end   = dofm(`mmax' + 1) - 1
    su __start, meanonly
    local dmin = r(mean)
    su __end, meanonly
    local dmax = r(mean)
    drop __start __end

    clear
    set obs `= `dmax' - `dmin' + 1'
    gen double d = `dmin' + _n - 1
    format d %td

    gen byte is_weekend = inlist(dow(d), 0, 6)
    merge m:1 d using "`feriados'", keep(master match) nogen keepusing(holiday_flag)
    gen byte is_holiday = (holiday_flag==1)
    replace is_holiday = 0 if missing(is_holiday)
    drop holiday_flag

    gen byte is_busday = (is_weekend==0 & is_holiday==0)
    sort d
    gen long bdcum = sum(is_busday)
    gen int m = mofd(d)
    format m %tm

    tempfile bizcal
    save "`bizcal'", replace
restore

preserve
    use "`bizcal'", clear
    keep if is_busday==1
    bysort m (d): keep if _n==_N
    keep m d
    rename d lbd
    tempfile lbd
    save "`lbd'", replace
restore


********************************************************************************
* 6) PATCHED MERGE BLOCK — alinhar chaves antes dos merges
********************************************************************************
capture confirm variable obs_m
if _rc {
    di as error "obs_m não está disponível no dataset ativo."
    describe
    exit 111
}

preserve
    use "`lbd'", clear
    rename m obs_m
    save "`lbd'", replace
restore
merge m:1 obs_m using "`lbd'", keep(master match) nogen

preserve
    use "`bizcal'", clear
    rename d maturity_d
    keep maturity_d bdcum
    tempfile bizcal_by_mat
    save "`bizcal_by_mat'", replace
restore
merge m:1 maturity_d using "`bizcal_by_mat'", keep(master match) nogen
rename bdcum bdcum_mat

preserve
    use "`bizcal'", clear
    rename d lbd
    keep lbd bdcum
    tempfile bizcal_by_lbd
    save "`bizcal_by_lbd'", replace
restore
merge m:1 lbd using "`bizcal_by_lbd'", keep(master match) nogen
rename bdcum bdcum_obs

gen long   tenor_busdays = bdcum_mat - bdcum_obs
replace    tenor_busdays = 0 if tenor_busdays<0 | missing(tenor_busdays)
gen double tenor_years_bd = tenor_busdays/252
label var tenor_years_bd "Tenor (anos) - ACT/252 (LBDoM→maturity)"
label var lbd            "Último dia útil do mês (referência)"


********************************************************************************
* 7) Checagens rápidas e salvar (pré-filtro)
********************************************************************************
summarize spread
_pctile spread, p(1 5 50 95 99)
di as res "p1=" r(r1) "  p5=" r(r2) "  p50=" r(r3) "  p95=" r(r4) "  p99=" r(r5)

capture noisily tab issuer
capture noisily tab sector
capture noisily tab bond_type

save "`path_out'", replace
di as res ">> Base limpa salva em: `path_out'"


* ============================
* 7B) FILTRAR #N/A EM FUNDAMENTAIS (FIX)
*     MODO PADRÃO: drop se QUALQUER dos 3 estiver missing
*     (se quiser manter linhas com ao menos 1 disponível, troque para a linha 'all-missing')
* ============================
use "`path_out'", clear
count
di as res ">> Total original: " r(N)

* --- MODO PADRÃO (recomendado p/ regressões): exigir todos presentes
drop if missing(bs_tot_asset) | missing(cf_cash_from_oper) | missing(tot_debt_to_ebitda)

* --- Alternativa (menos restrita): só remove se todos ausentes
* drop if missing(bs_tot_asset) & missing(cf_cash_from_oper) & missing(tot_debt_to_ebitda)

count
di as res ">> Após filtro (fundamentais disponíveis): " r(N)
save "`path_out'", replace
di as res ">> Base limpa atualizada com filtro de #N/A nas variáveis fundamentais."


* ================================
* 8) ENHANCEMENTS — escala, winsor, OLS/FE/GMM e exportação
* ================================
use "`path_out'", clear
xtset id obs_m

* ---- 8.1) Rótulos amigáveis ----
label var spread              "Corporate Spread (bps)"
label var vix                 "VIX (global risk)"
label var fed_funds_rate      "Fed Funds Rate (%)"
label var usd_brl             "USD/BRL"
label var ipca_yoy            "IPCA YoY (%)"
label var gdp_yoy             "GDP YoY (%)"
label var sov_spread_5y       "Sovereign 5y CDS (bp)"
label var debt_gdp            "Debt/GDP (%)"
capture confirm variable tenor_years_bd
if !_rc label var tenor_years_bd "Tenor ACT/252 (anos, LBDoM→Mat)"
label var bs_tot_asset        "Total Assets (BS)"
label var cf_cash_from_oper   "Cash from Operations (CF)"
label var tot_debt_to_ebitda  "Total Debt / EBITDA"

* ---- 8.2) Reescala: BS_TOT_ASSET para bilhões de R$ ----
capture drop bs_tot_asset_bil
gen double bs_tot_asset_bil = bs_tot_asset/1e9
label var bs_tot_asset_bil "Total Assets (R$ bi)"

* ---- 8.3) Winsorização leve (1%–99%) ----
program drop _all
program define __winsor_1_99
    syntax varlist
    tempname p1 p99
    foreach v of local varlist {
        capture confirm numeric variable `v'
        if _rc continue
        quietly _pctile `v', p(1 99)
        scalar `p1'  = r(r1)
        scalar `p99' = r(r2)
        capture drop `v'_w
        gen double `v'_w = `v'
        replace `v'_w = `p1'  if `v'_w <  `p1'
        replace `v'_w = `p99' if `v'_w >  `p99'
        label var `v'_w "`: var label `v'' (winsor 1-99)"
    }
end

local WVARS spread vix fed_funds_rate usd_brl ipca_yoy gdp_yoy ///
             sov_spread_5y debt_gdp bs_tot_asset_bil ///
             cf_cash_from_oper tot_debt_to_ebitda
capture noisily __winsor_1_99 `WVARS'

* ---- 8.4) Regressões ----
local XW vix_w fed_funds_rate_w usd_brl_w ipca_yoy_w gdp_yoy_w ///
         sov_spread_5y_w debt_gdp_w bs_tot_asset_bil_w ///
         cf_cash_from_oper_w tot_debt_to_ebitda_w
local TENOR tenor_years_bd

local outtab "C:\Users\tsiqueira4\Downloads\spread-model-master\light_spread-model-master\datos_y_modelos\output\tables"
capture mkdir "`outtab'"

estimates clear
capture noisily regress spread_w `XW' `TENOR', vce(cluster id)
estimates store OLS_main

* FE (opcional)
* capture noisily xtreg spread_w `XW' `TENOR', fe vce(cluster id)
* estimates store FE_main

* Painel dinâmico
tempname gmm_ok
scalar `gmm_ok' = 0
capture which xtabond2
if !_rc {
    capture noisily xtabond2 spread_w L.spread_w `XW' `TENOR', ///
        gmm(L.spread_w, lag(1 2) collapse) ///
        iv(`XW' `TENOR') ///
        twostep robust small
    if !_rc {
        estimates store GMM_dyn
        scalar `gmm_ok' = 1
    }
}
if (`gmm_ok'==0) {
    capture noisily xtabond spread_w L.spread_w `XW' `TENOR', ///
        lags(1) twostep robust
    if !_rc estimates store GMM_dyn
}

* ---- 8.5) Exportação das tabelas ----
capture which esttab
if !_rc {
    esttab OLS_main using "`outtab'\ols_main_enhanced.rtf", replace ///
        b(%9.3f) se(%9.3f) star(* 0.10 ** 0.05 *** 0.01) ///
        stats(N r2, fmt(%9.0f %9.3f) labels("Obs." "R^2")) ///
        label title("Pooled OLS com fundamentais (winsor 1–99)")

    * esttab OLS_main FE_main using "`outtab'\ols_vs_fe_enhanced.rtf", replace ///
    *     b(%9.3f) se(%9.3f) star(* 0.10 ** 0.05 *** 0.01) ///
    *     stats(N r2 r2_within, fmt(%9.0f %9.3f %9.3f) labels("Obs." "R^2" "R^2 (within)")) ///
    *     mtitle("OLS" "FE") label title("OLS vs FE (cluster id)")

    capture confirm estimation results GMM_dyn
    if !_rc {
        esttab GMM_dyn using "`outtab'\gmm_dyn_enhanced.rtf", replace ///
            b(%9.3f) se(%9.3f) star(* 0.10 ** 0.05 *** 0.01) ///
            stats(N, fmt(%9.0f) labels("Obs.")) ///
            label title("Painel Dinâmico (Arellano–Bond)")
    }
}
else {
    preserve
        matrix b = e(b)'
        matrix V = e(V)
        mata: st_matrix("se", sqrt(diagonal(st_matrix("V"))))
        mata: st_matrix("p", 2:* (1:-normal(abs(st_matrix("b"):/st_matrix("se")))) )
        clear
        svmat double b, names(col)
        rename b1 coef
        svmat double se, names(col)
        rename se1 se
        svmat double p, names(col)
        rename p1 pvalue
        local k : colnames e(b)
        gen str32 var = ""
        local i = 1
        foreach nm of local k {
            replace var = "`nm'" in `i'
            local ++i
        }
        order var coef se pvalue
        export delimited using "`outtab'\ols_main_enhanced_basic.csv", replace
    restore
}

display as res ">> Tabelas exportadas em: `outtab'"


* ================================
* 8B) Painel dinâmico (Arellano–Bond) e exportação Tabela R2
* ================================
* 0) Panel setup (safe if T already exists)
capture confirm variable T
if _rc bys id: gen T = _N
keep if T >= 3
xtset id obs_m

* 1) Dynamic panel, tight instruments (xtabond2)
*    - usa spread_w, L.spread_w e dois regressors ilustrativos (ajuste se quiser incluir mais X)
xtabond2 spread_w L.spread_w sov_spread_5y_w usd_brl_w, ///
    gmm(L.spread_w, lag(2 3) collapse) ///
    iv(sov_spread_5y_w usd_brl_w, eq(level)) ///
    twostep robust small

* 2) Store and export Table R2
estimates store GMM_dyn

* Saída (use um caminho curto e certo para evitar r(603))
local outtab "C:\temp"
capture mkdir "`outtab'"
cd "`outtab'"

* Tabela R1 (já feita acima) fica igual; aqui só R2
esttab GMM_dyn using "gmm_dyn_enhanced.rtf", replace ///
    b(%9.3f) se(%9.3f) star(* 0.10 ** 0.05 *** 0.01) ///
    stats(N ar1p ar2p hansenp, labels("Obs." "AR(1) p" "AR(2) p" "Hansen p")) ///
    label title("Panel dinámico (Arellano–Bond, dos pasos)")

display as res ">> Tabela R2 exportada em: `outtab'\gmm_dyn_enhanced.rtf"


coefplot OLS_main, ///
    keep(vix_w fed_funds_rate_w usd_brl_w ipca_yoy_w gdp_yoy_w ///
         sov_spread_5y_w debt_gdp_w bs_tot_asset_bil_w ///
         cf_cash_from_oper_w tot_debt_to_ebitda_w) ///
    xline(0, lcolor(red)) ciopts(recast(rcap) lcolor(gs8)) ///
    msymbol(O) mcolor(blue) ///
    xlabel(, grid) ///
    title("Figura R1. Coeficientes normalizados por tipo de factor", size(medsmall)) ///
    ysize(4) xsize(6) ///
    scheme(s1mono)

	
