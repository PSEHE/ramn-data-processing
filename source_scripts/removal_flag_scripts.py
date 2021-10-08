get_removal_flags <- function(in.data, in.pollutant){
  
  print(paste('Generating removal flags for', in.pollutant))
  
  extremes.data <- in.data %>%
    rename('pol'=in.pollutant)
  
  hourly.distribution.net <- group_by(extremes.data, date_hour) %>%
    mutate(net_hrly_q1 = quantile(pol, 0.25, na.rm = T), 
           net_hrly_q3 = quantile(pol, 0.75, na.rm = T), 
           net_hrly_iqr = IQR(pol, na.rm = T),
           lower_fence = net_hrly_q1 - 3*net_hrly_iqr,
           upper_fence = net_hrly_q3 + 3*net_hrly_iqr) %>%
    ungroup()
  
  hourly.distribution.aqy.avg <- hourly.distribution.net %>%
    group_by(ID, date_hour) %>%
    mutate(aqy_hrly_avg = mean(pol, na.rm = T)) %>%
    dplyr::select(ID, date_hour, net_hrly_q1, net_hrly_q3, net_hrly_iqr, aqy_hrly_avg, upper_fence, lower_fence) %>%
    unique()
  
  hourly.extremes <- mutate(hourly.distribution.aqy.avg,
                            dist_extreme = ifelse(aqy_hrly_avg >= upper_fence | aqy_hrly_avg <= lower_fence, 1, 0),
                            flatline = ifelse(aqy_hrly_avg == 0, 1, 0)) ## To do - add additional logic to flag flatlining sensors for long periods
  
  running.extremes.dist <- group_by(hourly.extremes, ID) %>%
    mutate(run_id = rleid(dist_extreme)) %>%
    add_count(ID, run_id) %>%
    ungroup()
  
  running.extremes <- mutate(running.extremes.dist, aqy_remove_dist = ifelse(dist_extreme == TRUE & n  >= 24, 1, 0))
  
  removal.flags <- dplyr::select(running.extremes, c('ID', 'date_hour', 'aqy_remove_dist'))
  colnames(removal.flags) <- str_replace_all(colnames(removal.flags), 'aqy', in.pollutant)
  
  return(removal.flags)
  
}

def flag_for_removal(averaged_data):

	hourly_distribution_network = averaged_data.groupby('Time').mean().describe(percentiles = [0.25, 0.5, 0.75]).reset_index()
	hourly_distribution = hourly_distribution_network.merge(averaged_data, on = 'Time')









