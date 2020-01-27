library(tidyverse)
library(openxlsx)

# Set working directory
setwd("~/Projects/Puerto_Rico_Coop-Conf-EDA/")

# Import data
dat <- read.xlsx("data/PR_Fish_Landings_Dataset_Nicolas_Gomez.xlsx", sheet=1, startRow = 2)

# Check out data
head(dat)[1:5]
tail(dat)[1:5]

# Remove last two columns that year and geartype
dat <- dat[-( (nrow(dat) - 1):nrow(dat)), ]

# Rename column one
names(dat)[1] <- "species"

# Rename columns so they are unique (multiple F_traps so changed lobster to L_traps)

# Set column rename
lcol_names <- c("2012_L_traps", "2013_L_traps", "2014_L_traps", "2015_L_traps", 
                "2016_L_traps", "2017_L_traps", "2018_L_traps")

# Find columns with '_F_traps'
indx <- grepl('_F_traps', colnames(dat))

# Rename with lcol_names
names(dat)[indx][seq(2, 14, 2)] <- lcol_names

# Gather "tidy" data
dat2 <- gather(dat, key = year_gear, value = effort, -species)

# New columns: year and gear type
dat2$year <- substr(dat2$year_gear, 1, 4)
dat2$gear <- substr(dat2$year_gear, 6, nchar(dat2$year_gear))

# Reorder dat2
dat2 <- select(dat2, year, gear, species, effort)

# Save Regression Data
write_csv(dat2, "data/species_gear_reg_data.csv")

