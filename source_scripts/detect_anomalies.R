
#install_github("twitter/AnomalyDetection")

library(AnomalyDetection)
library(tidyverse)
library(lubridate)
library(tidyr)



### AnomalyDetection function for one AQY
detect_anomalies_aqy <- function(in.data, pollutant.type, aqy.id){
  
  detection.data.aqy <- in.data[in.data$ID == aqy.id, c('Time', pollutant.type)]
  
  try({detected.anoms.aqy <- AnomalyDetectionTs(detection.data.aqy, max_anoms = .2, direction = 'pos', alpha = .05, longterm = F, piecewise_median_period_weeks = 2, plot = F)
    
      detected.anoms.aqy <- detected.anoms.aqy$anoms %>%
        mutate(ID = aqy.id, anomaly = TRUE) %>%
        dplyr::select(ID, Time, anomaly)
    
    return(detected.anoms.aqy)
    }, silent = T)
    
  detected.anoms.aqy <- data.frame(cbind(aqy.id, as.character('2030-01-01 12:00:00'), NA))
  
  colnames(detected.anoms.aqy) <- c('ID', 'Time', 'anomaly')
    
  return(detected.anoms.aqy)
  
}



### AnomalyDetection function to check all AQYs
detect_anomalies_network <- function(in.data, pollutant.type){
  
  detection.aqys <- unique(in.data$ID)
  
  detected.anoms.net <- data.frame()
  
  for(aqy in detection.aqys){
    
    print(paste('Detecting Anomalies for', aqy))
    
    detected.anoms.aqy <- detect_anomalies_aqy(in.data, pollutant.type, aqy)
    
    detected.anoms.net <- rbind(detected.anoms.net, detected.anoms.aqy)
    
  }
  
  return(detected.anoms.net)
  
}



### Join anomalies with network data
join_timeseries_anomalies <- function(in.data, anomaly.data, pollutant.type){
  
  no.detects <- filter(anomaly.data, is.na(anomaly)) %>%
    pull(ID) %>%
    unique()
  
  aqy.df.anoms <- filter(anomaly.data, !is.na(anomaly)) %>%
    full_join(., in.data, by = c('Time', 'ID'))
  
  aqy.df.anoms$anomaly <- replace_na(aqy.df.anoms$anomaly, FALSE)
  
  aqy.df.anoms <- mutate(aqy.df.anoms, anomaly = ifelse(ID %in% no.detects, NA, anomaly))
  
  aqy.df.anoms <- rename_with(aqy.df.anoms, .fn = ~paste(pollutant.type, .x, sep = '_'), .cols = 'anomaly')
  
  return(aqy.df.anoms)
  
}



### Mean-Based Anomaly Detection 
# net.avg.anomaly <- group_by(aqy.ts.anomalies, Time) %>%
#   add_count(name = 'aqys_measured') %>%
#   filter(PM25_anomaly == T) %>%
#   group_by(Time, aqys_measured) %>%
#   count(name= 'aqys_anomaly') %>%
#   mutate(aqys_pct_anomaly= round(aqys_anomaly/aqys_measured, 2)) %>%
#   filter(aqys_pct_anomaly >= 0.5) %>%
#   ungroup() %>%
#   dplyr::select(Time) %>%
#   mutate(PM25_anomaly = TRUE)


