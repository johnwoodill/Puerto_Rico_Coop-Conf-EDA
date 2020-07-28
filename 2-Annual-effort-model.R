library(tidyverse)
library(lfe)
library(stargazer)

regdat = read_csv("~/Projects/Puerto_Rico_Coop-Conf-EDA/data/PR_regdat.csv")
unique(regdat$cc_ratio)
regdat = drop_na(regdat)


mod1 = felm(log(1 + effort) ~ sst + chlor_a + WSPD + noi, data = regdat)
summary(mod1)

mod2 = felm(log(1 + effort) ~ sst + chlor_a + WSPD + noi + cc_ratio, data = regdat)
summary(mod2)

mod3 = felm(log(1 + effort) ~ sst + chlor_a + WSPD + noi + cc_ratio + hurricane, data = regdat)
summary(mod3)

mod4 = felm(log(1 + effort) ~ sst + chlor_a + WSPD + noi + cc_ratio + hurricane | region, data = regdat)
summary(mod4)

mod5 = felm(log(1 + effort) ~ sst + chlor_a + WSPD + noi + cc_ratio + hurricane + year | region, data = regdat)
summary(mod5)

mod6 = felm(log(1 + effort) ~ sst + chlor_a + WSPD + noi + cc_ratio + hurricane | year + region, data = regdat)
summary(mod6)


multiply.100 <- function(x) (x * 100)

stargazer(mod1, mod2, mod3, mod4, mod5, mod6,
          align=FALSE, no.space=FALSE, style="aer", digits=2,
          title = "Econometric Model of Puerto Rico Fishing Effort",
          dep.var.labels = c("Log(Fishing Effort)", "Log(Fishing Effort)", "Log(Fishing Effort)", 
                             "Log(Fishing Effort)", "Log(Fishing Effort)"),
          model.names = FALSE, 
          omit.stat = c("ser", "f", "rsq"),
          covariate.labels = c("SST", "CHL", "Wind Speed", "NOI", "Conflict/Coop Ratio", "Hurricane", "Trend"),
          # apply.coef = multiply.100, apply.se = multiply.100,
          table.layout ="=dcm#-t-as=n",
          font.size = "footnotesize",
          add.lines = list(c("Region FE", "--", "--", "--", "Yes", "Yes", "Yes"),
                           c("Year FE", "--", "--", "--", "--", "Yes", "Yes")),
                           notes.append = FALSE, notes.align = "r")
