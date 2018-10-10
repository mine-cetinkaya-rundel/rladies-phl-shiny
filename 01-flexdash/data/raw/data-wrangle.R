library(fs)
library(tidyverse)
library(janitor)
library(here)

# load data --------------------------------------------------------------------

file_names <- dir_info(path = here("data/un-women/raw"), glob = "*csv")$path[1:11]

lookup <- read_csv(here("data/un-women/raw", "table-variable-lookup.csv"))

vars_to_keep <- c("Indicator Name", "Region", "Country Code", "Country", "Year", "Sex", "Value")

# read data function -----------------------------------------------------------

read_data <- function(file_name){
  
  current_data <- read_csv(file_name, skip = 2) %>%
    select(vars_to_keep) %>%
    janitor::clean_names()
  
  indicator <- unique(current_data$indicator_name)
  
  new_value_name <- lookup %>%
    dplyr::filter(table == indicator) %>%
    select(variable) %>%
    pull()
  
  names(current_data)[names(current_data) == "value"] = new_value_name
  
  current_data %>%
    select(-indicator_name) %>%
    mutate(key = paste(region, country_code, country, year, sex, sep = "--"))
  
}

# read 12 files in -------------------------------------------------------------

d1  <- read_data(file_names[1])
d2  <- read_data(file_names[2])
d3  <- read_data(file_names[3])
d4  <- read_data(file_names[4])
d5  <- read_data(file_names[5])
d6  <- read_data(file_names[6])
d7  <- read_data(file_names[7])
d8  <- read_data(file_names[8])
d9  <- read_data(file_names[9])
d10 <- read_data(file_names[10])
d11 <- read_data(file_names[11])

# combine data -----------------------------------------------------------------

d <- d1 %>%
  full_join(d2 %>% select(6:7), by = "key") %>%
  full_join(d3 %>% select(6:7), by = "key") %>%
  full_join(d4 %>% select(6:7), by = "key") %>%
  full_join(d5 %>% select(6:7), by = "key") %>%
  full_join(d6 %>% select(6:7), by = "key") %>%
  full_join(d7 %>% select(6:7), by = "key") %>%
  full_join(d8 %>% select(6:7), by = "key") %>%
  full_join(d9 %>% select(6:7), by = "key") %>%
  full_join(d10 %>% select(6:7), by = "key") %>%
  full_join(d11 %>% select(6:7), by = "key")
  
# separate key for missings ----------------------------------------------------

dd <- d %>%
  separate(key, c("region", "country_code", "country", "year", "sex"), 
           sep = "--", remove = FALSE) %>%
  select(region, country_code, country, year, sex, 
         hrs_unpaid_dom_care_work, prop_internet, 
         hrs_dom_care_work, labor_force, prop_self_employed, 
         prop_nat_parl, prop_female_judge, prop_family_worker, 
         prop_employers, perc_industrial, perc_service, key)

# save data --------------------------------------------------------------------

write_csv(dd, path = here("data/un-women/un-women.csv"))
