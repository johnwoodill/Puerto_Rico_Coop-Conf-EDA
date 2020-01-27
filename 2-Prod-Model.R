library(tidyverse)

# Import data
regdat <- read_csv("data/species_gear_reg_data.csv")

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

ggsave("figures/effort_gear_trends.png")

# Simple regression analysis
mod <- lm(log(1 + effort) ~ factor(gear) + factor(species) + factor(year), data = regdat)
summary(mod)



pred <- predict(mod)


pdat <- regdat %>% 
  group_by(year) %>% 
  summarise(effort = mean(effort))

ggplot(regdat, aes(x=year, y=effort, color=species)) + geom_point() + theme(legend.position = "none")



plot(density(log(1 + regdat$effort)))

