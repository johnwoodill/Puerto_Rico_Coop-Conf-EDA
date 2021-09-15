library(tidyverse)
library(stargazer)
library(fixest)
library(lfe)
library(performance)
library(see)
library(MASS)
library(rms)

regdat <- read_csv("~/Projects/Puerto_Rico_Coop-Conf-EDA/data/FULL_PR_regdat_monthly.csv")
regdat <- filter(regdat, year <= 2017)
regdat$fishing_effort <- log(1 + regdat$pounds / regdat$trips)

mod <- felm(fishing_effort ~ conflict_mean + coop_mean + rcs(sst_mean, 3) +
            rcs(chlor_a_mean, 3) + noi + hurricane + 
            rcs(year, 3) | month + region, data = regdat, weights=regdat$area)

summary(mod)


mod <- felm(fishing_effort ~ conflict_mean + coop_mean + sst_mean +
            chlor_a_mean + noi + hurricane + 
            year | month + region, data = regdat, weights=regdat$area)

summary(mod)

mod <- lm(fishing_effort ~ conflict_mean + coop_mean + rcs(sst_mean, 3) +
            rcs(chlor_a_mean, 3) + WSPD + noi + hurricane + 
            rcs(year, 3) , data = regdat, weights=regdat$area)

summary(mod)

check_model(mod)


cor(regdat$conflict, regdat$coop)   #.1751365

regdat$cc_ratio1 <- ifelse(is.infinite(regdat$cc_ratio1), 0, regdat$cc_ratio1)

regdat$effort <- regdat$pounds/regdat$trips

mod1 <- felm(fishing_effort ~ conflict_mean + coop_mean +  sst_mean + sla + chlor_a_mean + WSPD + noi, data = regdat)
summary(mod2)

mod2 <- felm(fishing_effort ~ conflict_mean + coop_mean +  sst_mean + sla + chlor_a_mean + WSPD + noi + hurricane, data = regdat)
summary(mod3)

mod3 <- felm(fishing_effort ~ conflict_mean + coop_mean +  sst_mean + sla + chlor_a_mean + WSPD + noi + hurricane | month, data = regdat)
summary(mod4)

mod4 <- felm(fishing_effort ~ conflict_mean + coop_mean +  sst_mean + sla + chlor_a_mean + WSPD + noi + hurricane | region + month, data = regdat)
summary(mod5)

mod5 <- felm(fishing_effort ~ conflict_mean + coop_mean +  sst_mean + sla + chlor_a_mean + WSPD + noi + hurricane | year + region + month, data = regdat)
summary(mod6)

mod6 <- felm(fishing_effort ~ conflict_mean + coop_mean +  sst_mean + sla + chlor_a_mean + WSPD + noi + hurricane + year | region + month, data = regdat)
summary(mod6)

mod7 <- felm(fishing_effort ~ conflict_mean + coop_mean +  sst_mean + sla + chlor_a_mean + WSPD + noi + hurricane + rcs(year, 3) | region + month, data = regdat)
summary(mod7)



multiply.100 <- function(x) (x * 100)

stargazer(mod1, mod2, mod3, mod4, mod5, mod6, mod7,
          align=FALSE, no.space=FALSE, style="aer", digits=2,
          title = "Model of Puerto Rico Fishing Effort",
          dep.var.labels = c("Log(Fishing Effort)", "Log(Fishing Effort)", "Log(Fishing Effort)", 
                             "Log(Fishing Effort)", "Log(Fishing Effort)", "Log(Fishing Effort)", "Log(Fishing Effort)"),
          model.names = FALSE, 
          omit.stat = c("ser", "f", "rsq"),
          covariate.labels = c("Avg. Conflict Int.", "Avg. Coop. Int.", "SST", "SSH", "CHL", "Wind Speed", "NOI", "Hurricane", "Linear Trend", "RCS Trend", "RCS Trend'"),
          apply.coef = multiply.100, apply.se = multiply.100,
          table.layout ="=dcm#-t-as=n",
          font.size = "footnotesize",
          add.lines = list(c("Month FE", "--", "--", "Yes", "Yes", "Yes", "Yes", "Yes"),
                           c("Region FE", "--", "--", "--", "Yes", "Yes", "Yes", "Yes"),
                           c("Year FE", "--", "--", "--", "--", "Yes", "--", "--")), 
          notes = c("Coefficients and S.E. have been multiplied by 100. Interpration is in percentage change."),
          notes.append = TRUE, notes.align = "r")


10^(3.0 / 250.0 * 1 - 4.2)
library(fastDummies)
library(ordinal)



regdat <- read_csv("~/Projects/Puerto_Rico_Coop-Conf-EDA/data/UNAGG_PR_regdat_monthly.csv")
regdat <- filter(regdat, year <= 2017)
regdat$fishing_effort <- log(1 + (regdat$pounds / regdat$trips))
regdat$cc <- ifelse(regdat$intensity <= -1, "conflict", "coop")
# regdat$intensity <- factor(regdat$intensity, levels = c(5, 4, 3, 2, 1, -1, -2, -3, -4, -5))


mod <- felm(fishing_effort ~ sst_mean + chlor_a_mean + noi + hurricane + year + I(year^2)
             | month + region + cc, data = regdat)

summary(mod)

getfe(mod)


ggplot(regdat, aes(intensity, fishing_effort)) + geom_point() + geom_smooth(method="lm") 

ndat = regdat %>% group_by(intensity) %>% summarise(fishing_effort = sum(fishing_effort))

ggplot(ndat, aes(intensity, fishing_effort)) + geom_line()

ttest <- regdat %>% group_by(intensity) %>% summarise(nn=n())

ggplot(ttest, aes(intensity, nn)) + geom_point() + geom_line()


#    intensity    nn
#    <fct>     <int>
#  1 5           168
#  2 4            20
#  3 3           219
#  4 2           148
#  5 1             4
#  6 -1           16
#  7 -2           85
#  8 -3          311
#  9 -4           47
# 10 -5           95

regdat$cc <- ifelse(regdat$intensity <= -1, 0, 1)

mod <- lm(fishing_effort ~ factor(intensity), data = regdat)

ggplot(data = NULL, aes(c(-4, -3, -2, -1, 1, 2, 3, 4, 5), y=mod$coefficients[2:10] + mod$coefficients[1])) + 
  geom_point() + 
  geom_line() +
  scale_x_continuous(breaks = c(-4, -3, -2, -1, 1, 2, 3, 4, 5))


#

omod <- polr(intensity ~  fishing_effort + sst_mean + sla + chlor_a_mean + WSPD + noi + hurricane, data = regdat, Hess = TRUE)

omod <- polr(intensity ~  fishing_effort, data = regdat, Hess = TRUE)
summary(omod)

testdata <- regdat %>% dplyr::select(-intensity) %>% group_by(region) %>% summarise_all(.funs = mean)

simdat = testdata
for (i in seq(0, 0.5, 0.01)){
  print(i)
  testdata2 = testdata
  testdata2$fishing_effort <- testdata2$fishing_effort*(1+i)
  npred = predict(omod, newdata=testdata2, type='p')
  
  simdat$newcol = colnames(npred)[apply(npred, 1, which.max)]

  names(simdat)[ncol(simdat)] <- paste0("change_", i)
}
  
ggplot(testdata, aes(region, pred)) + geom_point()



View(testdata)


omod <- polr(intensity ~  fishing_effort, data = regdat, Hess = TRUE)
summary(omod)

newdata = regdat %>% summarise(fishing_effort = mean(fishing_effort))

pred = predict(omod, newdata=newdata, type='probs')
max(pred)
plot(pred)
lines(pred)





100*(exp(omod$coefficients) - 1)

#
fit_1 <- stan_polr


omod$contrasts






# regdat <- dummy_cols(regdat, select_columns = 'intensity')


mod <- feols(fishing_effort ~ cc + sst_mean + sla + chlor_a_mean + WSPD + noi + hurricane + year 
             | month + region, data = regdat)

summary(mod)

mod$coefficients[8] * c(5, 4, 3, 2, 1, -1, -2, -3, -4, -5)

plot(mod$coefficients[8] * c(5, 4, 3, 2, 1, -1, -2, -3, -4, -5))
lines(mod$coefficients[8] * c(5, 4, 3, 2, 1, -1, -2, -3, -4, -5))

plot(mod$coefficients[8:15]*100)

mod$collin.coef

summary(mod)

mod$sumFE

mod <- lm(fishing_effort ~ sst_mean + sla + chlor_a_mean + WSPD + noi + hurricane + year + 
             factor(intensity), data = regdat, weights=regdat$area)
summary(mod)
