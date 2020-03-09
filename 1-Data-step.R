library(tidyverse)
library(openxlsx)

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





# Import conflict/coop data
idat <- read.xlsx("https://oregonstate.box.com/shared/static/qrg42kvnpcrkfbskns9f9jc2fgx6cuut.xlsx", detectDates = TRUE)


test <- idat[3, ]

i = 3

retdat <- data.frame()
for (i in 1:nrow(idat)){
  indat <- idat[i, ]
  nregions <- strsplit(indat$DNER_Districts, ",")
  
  odat <- data.frame()
  for (j in c("east", "west", "north", "south")){
    begDate <- paste0(substr(indat$StartDate, 1, 7), "-01")
    endDate <- paste0(substr(indat$EndDate, 1, 7), "-01")
    if (begDate <= endDate){
      int_date <- seq(as.Date(begDate), as.Date(endDate), by="month")
      ndat <- data.frame(date = int_date, region = j, aggIS = indat$Agg_Intensity_Score, CoopCon = indat$CoopCon)
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


pccdat1 <- filter(pccdat1, year >= 2010)
ggplot(pccdat1, aes(year, value, fill=con_coop))+ 
  geom_bar(stat="identity") + 
  labs(x=NULL, y="Number of Events (months)") + 
  theme_bw() +
  scale_x_continuous(breaks = c(2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019)) +
  NULL

ccdat1$year <- as.character(ccdat1$year)
regdat <- left_join(dat4, ccdat1, by = c("year", "region"))

# cc = conflict/coop ration
# Increase in cc causes a decline in revenue
regdat$ccdiv <- (regdat$con_sum/regdat$coop_sum)

regdat$ccdiv <- abs(regdat$int_con_sum)/regdat$int_coop_sum

# Simple linear model
mod <- lm(log(rev_sum) ~ ccdiv + factor(year) + factor(region), data = regdat)
summary(mod)

# Get effect
cceffect <- mod$coefficients[2] * regdat$ccdiv

# Build data frame of predictions
preddat <- data.frame(peffort = predict(mod), cceffect = cceffect, region = regdat$region, year = regdat$year)

# Get residuals into predict values (true values)
preddat$peffort_total <- preddat$peffort + residuals(mod)

# Remove effect from conf/coop (share of conflict)
preddat$peffort_cc <- preddat$peffort + residuals(mod) - preddat$cceffect

ggplot(preddat, aes(year, peffort_total, color=region, group=region)) + 
  geom_point() + 
  geom_line() +
  geom_point(aes(year, peffort_cc)) +
  geom_line(aes(year, peffort_cc), linetype='dashed') +
  geom_line(data=regdat, aes(year, log(rev_sum))) +
  facet_wrap(~region) +
  theme(legend.position = "none") +
  labs(x=NULL, y="Log(Total Revenue)") +
  NULL


# Amount of conflict revenue
ccrev <- data.frame(year = regdat$year, region = regdat$region, peffort = predict(mod))
ccrev$peffort <- ccrev$peffort + residuals(mod)
ccrev$cc_rev <- exp(cceffect)
dat4$rev_sum
ccrev$peffect_cc <- ccrev$peffort - abs(ccrev$cc_rev)
ccrev$diff_peffect_cc <- exp(ccrev$peffort - ccrev$peffect_cc)

ggplot(ccrev, aes(year, diff_peffect_cc)) + geom_bar(stat="identity")

  
ggplot(preddat, aes(year, exp(abs(cceffect)), fill=region)) + geom_bar(stat="identity")



# Checl begDate is <= endDate
for (i in 1:nrow(idat)){
  indat <- idat[i, ]
  record_check <- as.Date(indat$StartDate) <= as.Date(indat$EndDate)
  if (record_check == "FALSE"){
    print(paste0(i, "-", as.Date(indat$StartDate) <= as.Date(indat$EndDate)))
}}

ndat


# Get specific columns
idat <- select(idat, AggYearStart, AggYearEnd, DNER_Districts, CoopCon, CoopConIntensity, WBDisPR, LandName)

head(idat)[1:5]
tail(idat)[1:5]

idat$coop <- ifelse(idat$CoopCon == "coop", 1, 0)
idat$conf <- ifelse(idat$CoopCon == "con", 1, 0)


# Aggregate counts for each year, region
idat2 <- idat %>% 
  filter(DNER_Districts %in% c("north", "south", "east", "west")) %>% 
  group_by(AggYearStart, DNER_Districts) %>% 
  summarise(CoopInt_sum = sum(CoopConIntensity, na.rm=TRUE),
            CoopInt_mean = mean(CoopConIntensity, na.rm=TRUE),
            coop_sum = sum(coop, na.rm=TRUE),
            conf_sum = sum(conf, na.rm=TRUE))

names(idat2) <- c("year", "region", "coopInt_sum", "coopInt_mean", "coop_sum", "conf_sum")

# Convert long region to abbr.
idat2$region <- ifelse(idat2$region == "north", "N", 
                       ifelse(idat2$region == "east", "E",
                              ifelse(idat2$region == "south", "S", 
                                     ifelse(idat2$region == "west", "W", idat2$region))))

idat2$year <- as.character(idat2$year)


# Remove last two columns that year and geartype
dat <- dat[-( (nrow(dat) - 1):nrow(dat)), ]

# Rename column one
names(dat)[1] <- "species"


# Gather "tidy" data
dat2 <- gather(dat, key = year_region, value = effort, -species)

# New columns: year and gear type
dat2$year <- substr(dat2$year_region, 1, 4)
dat2$region <- substr(dat2$year_region, 6, 6)

# Reorder dat2
dat2 <- select(dat2, year, region, species, effort)


regdat <- left_join(dat2, idat2, by = c("year", "region"))

# Save Regression Data
write_csv(regdat, "data/effort_region_conflict_reg_data.csv")

# 
