library(tidyverse)
library(openxlsx)
library(lubridate)
library(rnoaa)

# install.packages("readxl")
library(readxl)

# Set working directory
setwd("~/Projects/Puerto_Rico_Coop-Conf-EDA/")

# Function to convert "west" to "W"
region_convert <- function(region){
  if (region == "west") return("W")
  if (region == "east") return("E")
  if (region == "north") return("N")
  if (region == "south") return("S")
  }


# -----------------------------------------------------
# Prices by species
# Get pricing data
pdat <- read.xlsx("https://oregonstate.box.com/shared/static/jrpl8gcfsjexmozughbw8m1m25w9wimh.xlsx", sheet=2, startRow = 2)


# Remove last two columns that year and geartype
pdat <- pdat[-( (nrow(pdat) - 1):nrow(pdat)), ]

# Rename column one
names(pdat)[1] <- "species"

# Record zeros to be NA
pdat[pdat == 0] <- NA

# Gather data and dropNA
pdat1 <- gather(pdat, -species, value=value, key = region)
pdat1 <- drop_na(pdat1)
pdat1$value <- as.numeric(pdat1$value)

# record year and region
pdat1$year <- substr(pdat1$region, 1, 4)
pdat1$region <- substr(pdat1$region, 6, 7)

# Get average prices
pdat2 <- pdat1 %>% 
  group_by(species, region) %>% 
  summarise(price = mean(value, na.rm=TRUE))
  
View(pdat2)


# -----------------------------------------------------------------------------------------
# Gasoline prices
gdat <- read_csv("https://oregonstate.box.com/shared/static/xys7i7tnu6d364yzrcbgt1kr21fvef29.csv")

gdat$month <- substr(gdat$date, 1, 3)
gdat$year <- substr(gdat$date, 5, 8)

gdat1 <- gdat %>% 
  group_by(year) %>% 
  summarise(gprices = sum(prices))


# -----------------------------------------------------------------------------------------
# Puerto Rico GDP
gdpdat <- read_csv("https://oregonstate.box.com/shared/static/47jk0ia85kq79hx8pv29s673qvha87c3.csv")
gdpdat <- filter(gdpdat, `Country Name` == "Puerto Rico")
gdpdat <- select(gdpdat, -c(`Country Code`, `Indicator Name`, `Indicator Code`))

gdpdat1 <- gather(gdpdat, -c(`Country Name`), key = year, value=gdp)
gdpdat1 <- select(gdpdat1, year, gdp)


# -----------------------------------------------------------------------------------------
# Puerto Rico Wind data

wdat <- read_csv("https://oregonstate.box.com/shared/static/6yxpd84e1olx7vzprspsitbb34m7qs5n.csv")
wdat




# -----------------------------------------------------
# Fishing effort by region
# Import effort and region data
dat <- read.xlsx("https://oregonstate.box.com/shared/static/jrpl8gcfsjexmozughbw8m1m25w9wimh.xlsx", sheet=1, startRow = 2)

# Remove last two columns that year and geartype
dat <- dat[-( (nrow(dat) - 1):nrow(dat)), ]

# Rename column one
names(dat)[1] <- "species"

# Gather data and dropNA
dat1 <- gather(dat, -species, value = value, key = region)
dat1$value <- as.numeric(dat1$value)

# record year and region
dat1$year <- substr(dat1$region, 1, 4)
dat1$region <- substr(dat1$region, 6, 7)

# Merge prices
dat2 <- left_join(dat1, pdat2, by=c("species", "region"))

# Drop NA prices
dat3 <- drop_na(dat2)

# Get revenue
dat3$rev <- dat3$value * dat3$price

dat4 <- dat3 %>% 
  group_by(year, region) %>% 
  summarise(rev_sum = sum(rev)) %>% 
  ungroup()



ggplot(dat4, aes(as.numeric(year), log(rev_sum), color=region, group=region)) + 
  geom_line() + 
  theme_bw() +
  geom_vline(xintercept = 2017, color='red', linetype="dashed") +
  # facet_grid(~region) +
  labs(x=NULL, y="Log(Total Revenue)") +
  scale_x_continuous(breaks = c(2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018)) +
  NULL


# 

mod <- felm(log(1 + rev_sum) ~ factor(region)| year, data = dat4)
summary(mod)


# ----------------------------------------------------------------
# Import conflict/coop data
idat <- read.xlsx("https://oregonstate.box.com/shared/static/qrg42kvnpcrkfbskns9f9jc2fgx6cuut.xlsx", detectDates = TRUE)

# Split up regions and events between start and end
retdat <- data.frame()
for (i in 1:nrow(idat)){
  indat <- idat[i, ]
  nregions <- strsplit(indat$DNER_Districts, ",")
  
  odat <- data.frame()
  for (j in nregions[[1]]){
    begDate <- paste0(substr(indat$StartDate, 1, 7), "-01")
    endDate <- paste0(substr(indat$EndDate, 1, 7), "-01")
    if (begDate <= endDate){
      int_date <- seq(as.Date(begDate), as.Date(endDate), by="month")
      ndat <- data.frame(eventID = indat$EventID, date = int_date, region = j, aggIS = indat$Intensity_Score, CoopCon = indat$CoopCon)
      odat <- rbind(odat, ndat)
    }
    
  retdat <- rbind(retdat, odat)
  }
}

# Count number of Coop/Conf per year
ccdat <- retdat

# Get year
ccdat$year <- year(ccdat$date)

# Unfactor region
ccdat$region <- as.character(ccdat$region)
ccdat$region <- gsub(" ", "", ccdat$region)

# record region to individual letter
ccdat$region <- ifelse(ccdat$region == "east", "E", ccdat$region)
ccdat$region <- ifelse(ccdat$region == "west", "W", ccdat$region)
ccdat$region <- ifelse(ccdat$region == "north", "N", ccdat$region)
ccdat$region <- ifelse(ccdat$region == "south", "S", ccdat$region)

# Identify coop/conf and region
ccdat$coop <- ifelse(ccdat$CoopCon == "coop", 1, 0)
ccdat$con <- ifelse(ccdat$CoopCon == "con", 1, 0)

# Get intensity for each
ccdat$int_coop <- ifelse(ccdat$aggIS > 0, ccdat$aggIS, 0)
ccdat$int_con <- ifelse(ccdat$aggIS < 0, ccdat$aggIS, 0)

# Aggregate counts to year/region
ccdat1 <- ccdat %>% 
  group_by(year, region) %>% 
  summarise(coop_sum = sum(coop),
            con_sum = sum(con),
            int_con_sum = sum(int_con),
            int_coop_sum = sum(int_coop))


pccdat1 <- gather(ccdat1, key = con_coop, value=value, -year, -region)


pccdat1 <- filter(pccdat1, year >= 2010 & con_coop %in% c("con_sum", "coop_sum"))

ggplot(pccdat1, aes(year, value, fill=con_coop))+ 
  geom_bar(stat="identity") + 
  labs(x=NULL, y="Number of Events (months)") + 
  theme_bw() +
  scale_x_continuous(breaks = c(2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019)) +
  facet_wrap(~region) +
  NULL

# --------------------------------------------------------------------------
# Merge in data before modeling check

# Merge in conflict data
ccdat1$year <- as.character(ccdat1$year)
regdat <- left_join(dat4, ccdat1, by = c("year", "region"))

# Merge in gas prices
regdat <- left_join(regdat, gdat1, by = "year")

# Merge in GDP
regdat <- left_join(regdat, gdpdat1, by = "year")

# cc = conflict/coop ration
# Increase in cc causes a decline in revenue
regdat$ccdiv <- (regdat$con_sum/regdat$coop_sum)
regdat$ccdiv <- ifelse(is.infinite(regdat$ccdiv), 0, regdat$ccdiv)

# regdat$ccdiv <- regdat$int_con_sum/regdat$int_coop_sum
# regdat$ccdiv <- ifelse(is.infinite(regdat$ccdiv), 0, regdat$ccdiv)

regdat$year <- as.numeric(regdat$year)


# Simple linear model
mod <- lm(log(1 + rev_sum) ~ ccdiv + factor(year) + factor(region), data = regdat)
summary(mod)

# Get effect
cceffect <- (abs(mod$coefficients[2])*regdat$rev_sum)



# Marginal effect
me <- ((exp(mod$coefficients[2]) - 1))
#   ccdiv  
# -37.19588 (%)

# Explained conflict in effort
cedat <- data.frame(conflict_effect = abs(me*regdat$ccdiv*regdat$rev_sum), year = regdat$year, region = regdat$region)
ggplot(cedat, aes(year, conflict_effect)) + 
  geom_bar(stat='identity') +
  labs(x=NULL, y="Conflict Effort (Revenue)") +
  scale_x_continuous(breaks = c(2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019))

# Get percentage of conflict/revenue
pcceffect <-cedat$conflict_effect/regdat$rev_sum

pcdat <- data.frame(year = regdat$year, region = regdat$region, pcceffect = pcceffect)

ggplot(pcdat, aes(year, pcceffect, color=region)) + 
  geom_point() + 
  geom_line() + 
  facet_wrap(~region, scales="free") +
  labs(x=NULL, y="Percentage of Revenue \n Explained by Conflict") +
  scale_x_continuous(breaks = c(2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019))



# Build data frame of predictions
preddat <- data.frame(peffort = predict(mod), cceffect = cceffect, region = regdat$region, year = regdat$year)

# Get residuals into predict values (true values)
preddat$peffort_total <- preddat$peffort + residuals(mod)

# Remove effect from conf/coop (share of conflict)
preddat$peffort_cc <- preddat$peffort - preddat$cceffect

ggplot(preddat, aes(year, peffort_total, color=region, group=region)) + 
  geom_point() + 
  geom_line() +
  geom_point(aes(year, peffort_cc)) +
  geom_line(aes(year, peffort_cc), linetype='dashed') +
  geom_line(data=regdat, aes(year, log(rev_sum))) +
  facet_wrap(~region) +
  theme(legend.position = "none") +
  labs(x=NULL, y="Log(Total Revenue)") +
  ylim(0, max(preddat$peffort_total)) +
  NULL


# Amount of conflict revenue
ccrev <- data.frame(year = regdat$year, region = regdat$region, peffort = predict(mod))
ccrev$peffort <- ccrev$peffort + residuals(mod)
ccrev$cc_rev <- exp(cceffect)
dat4$rev_sum
ccrev$peffect_cc <- ccrev$peffort - abs(ccrev$cc_rev)
ccrev$diff_peffect_cc <- exp(ccrev$peffort - ccrev$peffect_cc)

ggplot(ccrev, aes(year, diff_peffect_cc)) + geom_bar(stat="identity")


ggplot(regdat, aes(year, ccdiv, fill=region)) + 
  geom_bar(stat="identity") + 
  facet_wrap(~region, scale="free") +
  labs(x=NULL, y="Conflict/Cooperation Ratio") +
  scale_x_continuous(breaks = c(2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019))
  NULL

# Save Regression Data
write_csv(regdat, "data/effort_region_conflict_reg_data.csv")
