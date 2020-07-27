library(tidyverse)
library(lfe)

# Import data
regdat <- read_csv("data/species_gear_reg_data.csv")

# Region model
regdat <- read_csv("data/effort_region_conflict_reg_data.csv")

# Get n count of zero
regdat_ <- regdat %>% 
  dplyr::add_count(species, gear) %>% 
  group_by(species) %>% 
  mutate(nn = sum(effort == 0)) %>% 
  filter(nn <= 77/3) %>% 
  ungroup()

head(regdat_)

ggplot(regdat_, aes(year, log(1 + effort))) + 
  geom_line() + 
  facet_wrap(~species + gear) + 
  labs(x=NULL, y="Log (1 + effort)", title="Fish Catch by Species and Gear \n (data has 75% obs. != 0)") + 
  NULL

ggsave("figures/effort_gear_trends.png", width=10, height=10)


# Simple regression analysis
regdat2 <- drop_na(select(regdat, year, region, species, effort, coopInt_sum, coop_sum, conf_sum))
mod2 <- felm(log(1 + effort) ~ conf_sum | species + year + region, data = regdat2)
summary(mod2)

# Get region residuals
ndat <- data.frame(year = regdat2$year, region = regdat2$region, res =  mod2$residuals)
names(ndat)[3] <- "res"

ndat2 <- ndat %>% 
  group_by(year, region) %>% 
  summarise(sum_res = sum(res))

ggplot(ndat2, aes(x=region, y=sum_res, fill = region)) +
  geom_bar(stat="identity") + 
  facet_wrap(~year) + 
  labs(x="Region", y="Sum of Residuals by Region") +
  theme(legend.position = "none")


length(mod$coefficients)

coef <- mod$coefficients[132:(132-24)]
plot(coef)
lines(coef)


pred <- predict(mod)


pdat <- regdat %>% 
  group_by(year) %>% 
  summarise(effort = mean(effort))

ggplot(regdat, aes(x=year, y=effort, color=species)) + geom_point() + theme(legend.position = "none")



plot(density(log(1 + regdat$effort)))

