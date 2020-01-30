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
# Fishing effort by species
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




# -----------------------------------------------------
# Fishing effort by region
# Import effort and region data
dat <- read.xlsx("data/PR_Fish_Landings_Dataset_Nicolas_Gomez.xlsx", sheet=2, startRow = 2)

# Import conflict/coop data
idat <- read_csv("data/FCCE Form  2019 (Responses)_Aggregates_Expanded.csv")

# Get specific columns
idat <- select(idat, AggYearStart, AggYearEnd, DNER_Districts, CoopCon, CoopConIntensity, WBDisPR, LandName)

# Check out data
head(dat)[1:5]
tail(dat)[1:5]

head(idat)[1:5]
tail(idat)[1:5]

# Aggregate counts for each year, region
idat2 <- idat %>% 
  filter(DNER_Districts %in% c("north", "south", "east", "west")) %>% 
  group_by(AggYearStart, DNER_Districts) %>% 
  summarise(CoopInt_sum = sum(CoopConIntensity, na.rm=TRUE),
            CoopInt_mean = mean(CoopConIntensity, na.rm=TRUE))

names(idat2) <- c("year", "region", "coopInt_sum", "coopInt_mean")

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
