library(rnoaa)
library(tidyverse)
library(RCurl)
library(data.table)
library(zoo)

# Set tryCatch settings for downloading missing files
oldw <- getOption("warn")
options(warn = -1)

setwd("~/Projects/Puerto_Rico_Coop-Conf-EDA/")

# List of buoys used in analysis
#
# West
# PTRP4: 2012-2019
# 41115: 2011-2019
# MGZP4: 2010-2019
#
# North
# SJNP4: 2005-2019
# AROP4: 2010-2019

# South
# mgip4: 2010-2019 ?
# 42085: 2009-2019

# East
# 41052
# vqsp4
# frdp4
# 41056: 2013-2019
# CLBP4: 2009-2013; 2016-2019
# ESPP4: 2009-2019



# North PR Buoy Data
nbuoy <- c("sjnp4", "arop4", "41053")
years <- seq(2010, 2019, 1)
region <- "north"

for (buoy_id in nbuoy){
  for (year in years){
  # Download file and parse
  url <- paste0("https://www.ndbc.noaa.gov/data/historical/stdmet/", buoy_id, "h", year, ".txt.gz")
  destfile <- paste0("data/oceanographic/", buoy_id, "-", year, "-", region, ".txt.gz")
  download.file(url, destfile, method="libcurl")
  dat <- read_table2(destfile)
  dat <- dat[-1, ]
  dat$buoy_id <- buoy_id
  dat$region <- "north"
  dat <- as.data.frame(dat)
  write_csv(dat, paste0("data/oceanographic/processed/", buoy_id, "-", year, "-", region, ".csv"))
  print(paste0(buoy_id, "-", year))

  }
}

# South PR Buoy Data
nbuoy <- c("mgip4", "42085")
years <- seq(2010, 2019, 1)
region <- "south"

for (buoy_id in nbuoy){
  for (year in years){
  # Download file and parse
  url <- paste0("https://www.ndbc.noaa.gov/data/historical/stdmet/", buoy_id, "h", year, ".txt.gz")
  destfile <- paste0("data/oceanographic/", buoy_id, "-", year, "-", region, ".txt.gz")
  download.file(url, destfile, method="libcurl")
  dat <- read_table2(destfile)
  dat <- dat[-1, ]
  dat$buoy_id <- buoy_id
  dat$region <- region
  dat <- as.data.frame(dat)
  write_csv(dat, paste0("data/oceanographic/processed/", buoy_id, "-", year, "-", region, ".csv"))
  print(paste0(buoy_id, "-", year))

  }
}

# GHCND:RQW00011630

# East PR Buoy Data
nbuoy <- c("41056", "41052", "clbp4", "espp4", "vqsp4", "frdp4")
years <- seq(2010, 2019, 1)
region <- "east"

for (buoy_id in nbuoy){
  for (year in years){
  # Download file and parse
  url <- paste0("https://www.ndbc.noaa.gov/data/historical/stdmet/", buoy_id, "h", year, ".txt.gz")
  destfile <- paste0("data/oceanographic/", buoy_id, "-", year, "-", region, ".txt.gz")
  err <- try(download.file(url, destfile, method="libcurl"))
  if (class(err) != "try-error"){
    dat <- read_table2(destfile)
    dat <- dat[-1, ]
    dat$buoy_id <- buoy_id
    dat$region <- region
    dat <- as.data.frame(dat)
    write_csv(dat, paste0("data/oceanographic/processed/", buoy_id, "-", year, "-", region, ".csv"))
    print(paste0(buoy_id, "-", year))
  }
  }
}
fas


# West PR Buoy Data
nbuoy <- c("ptrp4", "41115", "mgzp4")
years <- seq(2010, 2019, 1)
region <- "west"

for (buoy_id in nbuoy){
  for (year in years){
  # Download file and parse
  url <- paste0("https://www.ndbc.noaa.gov/data/historical/stdmet/", buoy_id, "h", year, ".txt.gz")
  destfile <- paste0("data/oceanographic/", buoy_id, "-", year, "-", region, ".txt.gz")
  err <- try(download.file(url, destfile, method="libcurl"))
  if (class(err) != "try-error"){
    dat <- read_table2(destfile)
    dat <- dat[-1, ]
    dat$buoy_id <- buoy_id
    dat$region <- region
    dat <- as.data.frame(dat)
    write_csv(dat, paste0("data/oceanographic/processed/", buoy_id, "-", year, "-", region, ".csv"))
    print(paste0(buoy_id, "-", year))
  }
  }
}


# Bind all files
files <- list.files("data/oceanographic/processed/", full.names = TRUE)
bdat <- as.data.frame(rbindlist(lapply(files, read_csv), fill=TRUE))

# Second column
# #yr mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa degC degC  degC   mi    ft   41056   east
dat <- as_tibble(bdat)

# Fix first column
names(dat)[1] <- "year"

# Error cleaning
# WVHT = 99
# DPD = 99 
# APD = 99
# MWD = 999
# PRES = 9999
# ATMP = 999
# WTMP = 999 
# DEWP = 999 (no data)
# VIS  = 99 (no data)
# TIDE = 99 (no data)
# WDIR = 999
# WSPD = 99
# GST = 9

# Record to numerics
dat$WVHT <- as.numeric(dat$WVHT)
dat$DPD <- as.numeric(dat$DPD)
dat$APD <- as.numeric(dat$APD)
dat$MWD <- as.numeric(dat$MWD)
dat$PRES <- as.numeric(dat$PRES)
dat$ATMP <- as.numeric(dat$ATMP)
dat$WTMP <- as.numeric(dat$WTMP)
dat$DEWP <- as.numeric(dat$DEWP)
dat$VIS <- as.numeric(dat$WVHT)
dat$TIDE <- as.numeric(dat$TIDE)
dat$WDIR <- as.numeric(dat$WDIR)
dat$WSPD <- as.numeric(dat$WSPD)
dat$GST <- as.numeric(dat$GST)


# Record 9's to NA's
dat$WVHT <- ifelse(dat$WVHT == 99, NA, dat$WVHT)
dat$DPD <- ifelse(dat$DPD == 99, NA, dat$DPD)
dat$APD <- ifelse(dat$APD == 99, NA, dat$APD)
dat$MWD <- ifelse(dat$MWD == 999, NA, dat$MWD)
dat$PRES <- ifelse(dat$PRES == 9999, NA, dat$PRES)
dat$ATMP <- ifelse(dat$ATMP == 999, NA, dat$ATMP)

# dat$WTMP <- ifelse(dat$WTMP == 99, NA, dat$WTMP)
dat$WTMP <- ifelse(dat$WTMP == 999, NA, dat$WTMP)

dat$DEWP <- ifelse(dat$DEWP == 999, NA, dat$DEWP)
dat$VIS <- ifelse(dat$VIS == 99, NA, dat$VIS)
dat$TIDE <- ifelse(dat$TIDE == 99, NA, dat$TIDE)
dat$WDIR <- ifelse(dat$WDIR == 999, NA, dat$WDIR)
dat$WSPD <- ifelse(dat$WSPD == 99, NA, dat$WSPD)
dat$GST <- ifelse(dat$GST == 9, NA, dat$GST)

# Remove hour and mm
dat <- select(dat, -hh, -mm)

# Set date
dat$date <- paste0(dat$year, "-", dat$MM, "-01")
dat$date <- as.Date(dat$date)


# aggregate to monthly obs
# 1. Get average for each day
# 2. Get sum for all months

dat2 <- dat %>% 
  group_by(year, MM, DD, region) %>% 
  arrange(date) %>% 
  summarise_all(mean, na.rm=TRUE) %>% 
  ungroup()


# WDIR	Wind direction (the direction the wind is coming from in degrees clockwise from true N) during the same period used for WSPD. See Wind Averaging Methods
# WSPD	Wind speed (m/s) averaged over an eight-minute period for buoys and a two-minute period for land stations. Reported Hourly. See Wind Averaging Methods.
# GST	Peak 5 or 8 second gust speed (m/s) measured during the eight-minute or two-minute period. The 5 or 8 second period can be determined by payload, See the Sensor Reporting, Sampling, and Accuracy section.
# WVHT	Significant wave height (meters) is calculated as the average of the highest one-third of all of the wave heights during the 20-minute sampling period. See the Wave Measurements section.
# DPD	Dominant wave period (seconds) is the period with the maximum wave energy. See the Wave Measurements section.
# MWD	The direction from which the waves at the dominant period (DPD) are coming. The units are degrees from true North, increasing clockwise, with North as 0 (zero) degrees and East as 90 degrees. See the Wave Measurements section.
# ATMP	Air temperature (Celsius). For sensor heights on buoys, see Hull Descriptions. For sensor heights at C-MAN stations, see C-MAN Sensor Locations
# WTMP	Sea surface temperature (Celsius). For buoys the depth is referenced to the hull's waterline. For fixed platforms it varies with tide, but is referenced to, or near Mean Lower Low Water (MLLW).
# VIS	Station visibility (nautical miles). Note that buoy stations are limited to reports from 0 to 1.6 nmi.

dat2$date <- paste0(dat2$year, "-", dat2$MM, "-", dat2$DD)
dat2$date <- as.Date(dat2$date)

# Build data frame of all dates
mdat <- data.frame(date = rep(seq(as.Date("2010-01-01"), as.Date("2019-12-31"), by = "day"), 4),
                   region = rep(c("north", "south", "east", "west"), each = 3652, by=4))

mdat <- left_join(mdat, dat2, by = c("date", "region"))


# 3651 days from 2010-2019

# ----------------------------------------
# Amelia interpolation
library(Amelia)

dat3 <- select(mdat, date, region, WDIR, WSPD, GST, WVHT, DPD, MWD, ATMP, WTMP, VIS)

amres <- amelia(dat3, ts = c("date"), cs = c( "region"), m=1)

pdat <- amres$imputations$imp2

amres$orig.vars

which(is.na(pdat$WSPD))

which(pdat$)


# ----------------------------------------

pdat <- dat2

ggplot(pdat, aes(date, WSPD, color=factor(region), group=1)) + 
  geom_point() + 
  geom_line() + 
  facet_wrap(~region, scales="free") +
  theme(legend.position = "none") +
  NULL

ggplot(pdat, aes(date, WVHT, color=factor(region), group=1)) + 
  geom_point() + 
  geom_line() + 
  facet_wrap(~region, scales="free") +
  theme(legend.position = "none") +
  NULL

ggplot(pdat, aes(date, WTMP, color=factor(region), group=1)) + 
  geom_point() + 
  geom_line() + 
  facet_wrap(~region, scales="free") +
  theme(legend.position = "none") +
  NULL

