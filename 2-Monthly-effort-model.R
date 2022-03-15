library(tidyverse)
library(stargazer)
library(fixest)
library(lfe)
library(performance)
library(rms)


regdat <- read_csv("~/Projects/Puerto_Rico_Coop-Conf-EDA/data/FULL_PR_regdat_monthly.csv")

regdat$fishing_effort <- log( (regdat$pounds/ regdat$trips))

ggplot(regdat, aes(year, fishing_effort)) + geom_smooth() + theme_minimal() + facet_wrap(~region, scales='free')


regdat <- read_csv("~/Projects/Puerto_Rico_Coop-Conf-EDA/data/UNAGG_PR_regdat_monthly.csv")
regdat <- filter(regdat, year <= 2017)
regdat$fishing_effort <- log( (regdat$pounds/ regdat$fishers))
regdat <- dplyr::filter(regdat, intensity <= -3 | intensity >= 3)
regdat$cc_binary <- ifelse(regdat$intensity < 0, 1, 0)

ddat <- regdat %>% group_by(year) %>% summarise(count = sum(cc_binary))

ggplot(ddat, aes(year, count)) + geom_bar(stat='identity') + geom_smooth()



# -------------------------------[Testing models]
regdat <- read_csv("~/Projects/Puerto_Rico_Coop-Conf-EDA/data/UNAGG_PR_regdat_monthly.csv")
regdat <- filter(regdat, year <= 2017)

regdat$fishing_effort <- log( (regdat$pounds/ regdat$trips))
regdat <- dplyr::filter(regdat, intensity <= -3 | intensity >= 3)
regdat$cc_binary <- ifelse(regdat$intensity < 0, 1, 0)

mod <- felm(fishing_effort ~ sst_mean + I(sst_mean^2) + chlor_a_mean + I(chlor_a_mean^2) + 
              sla + noi + rcs(year, 3) + factor(region):cc_binary
             | month, data = regdat, weights=regdat$area)
summary(mod)


mod <- felm(fishing_effort ~ sst_mean + I(sst_mean^2) + chlor_a_mean + I(chlor_a_mean^2) + 
              sla + noi + rcs(year, 3) + factor(intensity) 
             | region + month, data = regdat, weights=regdat$area)

summary(mod)

mod <- felm(fishing_effort ~ sst_mean + I(sst_mean^2) + chlor_a_mean + I(chlor_a_mean^2) + 
              sla + noi + rcs(year, 3) + cc_binary
             | month, data = regdat, weights=regdat$area)
summary(mod)

ggplot(regdat, aes(cc_binary, fishing_effort, color=factor(intensity))) + geom_point() + geom_smooth(method="lm") 



# --------------------------------------------------------------
# Build up table
regdat <- read_csv("~/Projects/Puerto_Rico_Coop-Conf-EDA/data/UNAGG_PR_regdat_monthly.csv")
regdat <- filter(regdat, year <= 2017)

regdat$fishing_effort <- log( 1 + (regdat$pounds/ regdat$trips))
regdat <- dplyr::filter(regdat, intensity <= -3 | intensity >= 3)
regdat$cc_binary <- ifelse(regdat$intensity < 0, 1, 0)


# Simple model with weather (no fe)
mod1 <- felm(fishing_effort ~ cc_binary + sst_mean + I(sst_mean^2) + chlor_a_mean + I(chlor_a_mean^2) + sla + noi + hurricane, data = regdat)
summary(mod1)

# Add trend
mod2 <- felm(fishing_effort ~ cc_binary + sst_mean + I(sst_mean^2) + chlor_a_mean + I(chlor_a_mean^2) + sla + noi + hurricane| region + month, data = regdat, weights=regdat$area)
summary(mod2)

# add month and region fe
mod3 <- felm(fishing_effort ~ cc_binary + sst_mean + I(sst_mean^2) + chlor_a_mean + I(chlor_a_mean^2) + sla + noi + rcs(year, 3) + hurricane| region + month, data = regdat, weights=regdat$area)
summary(mod3)


mod3 <- felm(fishing_effort ~ cc_binary + sst_mean + I(sst_mean^2) + chlor_a_mean + I(chlor_a_mean^2) + sla + noi + rcs(year, 3) | region:cc_binary + month, data = regdat, weights=regdat$area)

mod3 <- felm(fishing_effort ~ sst_mean + I(sst_mean^2) + chlor_a_mean + I(chlor_a_mean^2) + sla + noi + rcs(year, 3) + hurricane + factor(region):cc_binary | month, data = regdat, weights=regdat$area)
summary(mod3)

ggplot(regdat, aes(year, fishing_effort)) + 
  geom_bar(stat='identity') +
  # geom_line(aes(year, cc_binary)) + 
  facet_wrap(~region, scales='free')

regdat2 = regdat %>% group_by(region, year) %>% summarise(cc_binary = sum(cc_binary),
                                                          sum_fishing_effort = sum(fishing_effort))

ggplot(regdat2, aes(year, cc_binary)) + geom_bar(stat='identity') + facet_wrap(~region, scales='free')

regdat3 = regdat %>% group_by(year, month) %>% mutate(sum_fishing_effort = sum(fishing_effort),
                                                      p_fishing_effort = fishing_effort/sum_fishing_effort) %>% 
  group_by(region, year) %>% summarise(p_fishing_effort = mean(p_fishing_effort))

ggplot(regdat3, aes(year, p_fishing_effort)) + geom_bar(stat='identity') + facet_wrap(~region, scales='free')

multiply.100 <- function(x) (x * 100)

stargazer(mod1, mod2, mod3,
          align=FALSE, no.space=FALSE, style="aer", digits=3,
          title = "Model of Puerto Rican Fishing Effort",
          dep.var.labels = c("Log(Fishing Effort)", "Log(Fishing Effort)",
                             "Log(Fishing Effort)"),
          model.names = FALSE, 
          omit.stat = c("ser", "f", "rsq"),
          covariate.labels = c("Conflict", "SST", "SST Sq.", "CHL", "CHL Sq.", "SSH", "NAO", "Hurricane"),
          omit = c("year"),
          # apply.coef = multiply.100, apply.se = multiply.100,
          table.layout ="=dcm#-t-as=n",
          font.size = "footnotesize",
          add.lines = list(c("Month FE", "--", "Yes", "Yes"),
                           c("Region FE", "--", "Yes", "Yes"),
                           c("NL Trend", "--", "--", "Yes")),
                           
          # notes = c("Coefficients and S.E. have been multiplied by 100. Interpration is in percentage change."),
          notes.append = TRUE, notes.align = "r")



