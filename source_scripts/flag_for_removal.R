
######### GET UPPER AND LOWER BOUNDS FOR FLAGS ###########

get_network_distribution <- function(data.unflagged){
  
  network.hourly <- data.unflagged %>%
    group_by(Time) %>%
    mutate(net_hrly_q1 = quantile(pollutant.to.flag, 0.25, na.rm = T), 
           net_hrly_q3 = quantile(pollutant.to.flag, 0.75, na.rm = T), 
           net_hrly_iqr = IQR(pollutant.to.flag, na.rm = T),
           lower_fence = net_hrly_q1 - 3*net_hrly_iqr,
           upper_fence = net_hrly_q3 + 3*net_hrly_iqr) %>%
    ungroup()
  
  return(network.hourly)
  
}

######### FLAG AQY BASED ON HOURLY VALUES ###########

flag_aqy_for_hour <- function(network.hourly){
  
  hourly.flags <- mutate(network.hourly, 
                         flag_dist = ifelse(pollutant.to.flag >= upper_fence | pollutant.to.flag <= lower_fence, 1, 0),
                         flag_flat = ifelse(pollutant.to.flag == 0, 1, 0))
  
}

######### COUNT HOW LONG HOURLY VALUES FLAGGED ###########

get_length_of_flags <- function(hourly.flags){

  length.of.flag <- hourly.flags %>%
    group_by(ID) %>%
    mutate(run_id_dist = rleid(flag_dist),
           run_id_flat = rleid(flag_flat)) %>%
    add_count(ID, run_id_dist, name = 'hours_flagged_dist') %>%
    add_count(ID, run_id_flat, name = 'hours_flagged_flat') %>%
    ungroup()
    
  return(length.of.flag)
  
}

######### FLAG FOR REMOVAL IF RUNNING HOURLY FLAGS TOO LONG ###########

decide_if_discard <- function(length.of.flags, in.pollutant){
  
  discard.yn <- length.of.flags %>%
    mutate(aqy_rmv_dist = ifelse(flag_dist == 1 & hours_flagged_dist >= 24, 1, 0),
           aqy_rmv_flat = ifelse(flag_flat == 1 & hours_flagged_flat >= 24, 1, 0))
  
  removal.flags <- discard.yn %>%
    dplyr::select(., c(ID, Time, aqy_rmv_dist, aqy_rmv_flat)) %>%
    rename_with(.cols = everything(), .fn = ~str_replace_all(.x, 'aqy', in.pollutant))
  
  return(removal.flags)
  
}

##########################################################
### PUT TOGETHER REMOVAL FUNCTION FOR EXTREME MEASURES ###
##########################################################


get_flags_for_pollutant <- function(data.unflagged, in.pollutant){
  
  data.unflagged.rename <- data.unflagged %>%
    rename('pollutant.to.flag'=in.pollutant)
  
  network.hourly <- get_network_distribution(data.unflagged.rename)
  
  hourly.flags <- flag_aqy_for_hour(network.hourly)
  
  length.flags <- get_length_of_flags(hourly.flags)
  
  removal.flags <- decide_if_discard(length.flags, in.pollutant)
  
  return(removal.flags)
  
}


######### CHECK FOR SENSORS WITH HIGH VALUES ###########


flag_high_measurements <- function(data.unflagged, in.pollutant){
  
  data.unflagged <- mutate(data.unflagged, time_month = substr(Time, 1, 7))
  
  data.flagged.high <- data.unflagged %>%
    group_by(ID, time_month) %>%
    add_count(name = 'obs') %>%
    mutate(NO2_high = ifelse(NO2 > 100, 1, 0), 
           PM25_high = ifelse(PM25 > 300, 1, 0), 
           O3_high = ifelse(O3 > 100, 1, 0)) %>%
    rename_with(.cols = everything(), .fn = ~str_replace_all(.x, in.pollutant, 'in_pollutant'))
  
  return(data.flagged.high)
  
}


######### CHECK FOR SENSORS WITH CONSISTENTLY HIGH VALUES THROUGH MONTH ###########


check_if_consistently_high <- function(data.flagged.high){
  
  data.flagged.high.count <- data.flagged.high %>%
    group_by(ID, time_month, obs) %>%
    summarize(obs_high = sum(in_pollutant_high), .groups = 'drop') %>%
    dplyr::select(., c(ID, time_month, obs, obs_high)) %>%
    unique()
  
  return(data.flagged.high.count)
  
}


######### CHECK FOR SENSORS WITH CONSISTENTLY HIGH VALUES THROUGH MONTH ###########


determine_months_to_remove <- function(data.flagged.high.count, in.pollutant){
  
  months.to.remove <- data.flagged.high.count %>%
    mutate(pct_high = round(obs_high/obs, 3),
           faulty = ifelse(pct_high >= 0.25, 1, 0)) %>%
    dplyr::select(c(ID, time_month, faulty))
  
  months.to.remove$faulty <- replace_na(months.to.remove$faulty, 0)
  
  colnames(months.to.remove) <- c('ID', 'time_month', paste0(in.pollutant, '_rmv_faulty'))
  
  if(nrow(months.to.remove) == 0){return(print(paste('No sensors require removal for', in.pollutant)))}
    else(return(months.to.remove))
  
}


##########################################################
###  PUT TOGETHER REMOVAL FUNCTION FOR FAULTY SENSORS  ###
##########################################################


check_for_faulty_sensor <- function(data.unflagged, in.pollutant){
  
  data.flagged.high <- flag_high_measurements(data.unflagged, in.pollutant)
  
  data.flagged.high.count <- check_if_consistently_high(data.flagged.high)
  
  months.to.remove <- determine_months_to_remove(data.flagged.high.count, in.pollutant)
  
  return(months.to.remove)
  
}


##########################################################################
##########################################################################
#################  PUT TOGETHER BOTH REMOVAL FUNCTIONS  ##################
##########################################################################
##########################################################################

flag_transient_and_lasting_errors <- function(data.unflagged, in.pollutant){
  
  print('Flagging short-term (24 hr) sensor errors')
  flags.transient <- get_flags_for_pollutant(data.unflagged, in.pollutant)
  
  print('Flagging long-term (monthly) errors')
  flags.lasting <- check_for_faulty_sensor(data.unflagged, in.pollutant)
  
  print('Joining flags and data')
  data.flagged <- mutate(data.unflagged, time_month = substr(Time, 1, 7)) %>%
    full_join(flags.transient, by = c('ID', 'Time')) %>%
    full_join(flags.lasting, by = c('ID', 'time_month')) %>%
    dplyr::select(c(ID, Time, contains(in.pollutant)))
  
  return(data.flagged)
  
}

























