
######### CALIBRATE DATA #########

calibrate_data <- function(in_data_raw, in_cal_params){
  
  data_raw_join_params <- inner_join(in_data_raw, in_cal_params, by = 'ID') %>%
    filter(Time >= start_date & Time <= end_date) %>%
    rename_with(.cols = c('O3', 'NO2', 'Ox', 'PM25', 'PM25_raw'), .fn = ~paste0(.x, '_precal'))
  
  data_calibrated <- mutate(data_raw_join_params,
                            O3 = (O3_precal-O3.offset)*O3.gain,
                            Ox = (Ox_precal-Ox.offset)*Ox.gain,
                            NO2 = Ox - NO2.a.value*O3,
                            NO2 = (NO2-NO2.offset)*NO2.gain,
                            PM25 = (PM25_precal-PM.offset)*PM.gain,
                            PM25_raw = (PM25_raw_precal-PM.offset)*PM.gain) %>%
    select(c(ID, Longitude, Latitude,Time, TEMP, RH, DP, O3_precal, O3, Ox_precal, Ox, NO2_precal, NO2, PM25_precal, PM25_raw_precal, PM25, PM25_raw)) %>%
    mutate(across(.cols = !starts_with(c('I', 'L', 'T', 'R', 'D')), .fn = ~round(.x, 1)))
  
  return(data_calibrated)
  
}