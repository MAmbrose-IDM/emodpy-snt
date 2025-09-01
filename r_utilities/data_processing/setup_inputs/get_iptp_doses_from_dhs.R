library(readstata13)
library(foreign)

get_iptp_doses_from_dhs = function(hbhi_dir, dta_dir, years, sim_start_year, last_sim_year, country='NGA'){
  if(!dir.exists(paste0(hbhi_dir,'/simulation_inputs/IPTp'))) dir.create(paste0(hbhi_dir,'/simulation_inputs/IPTp'))

  # calculate fraction of all individuals with each number of IPTp doses
  store_iptp_fractions = matrix(NA, ncol = length(years), nrow=3)
  for(yy in 1:length(years)){
    DHS_file_recode_df = read.csv(paste0(hbhi_dir, '/estimates_from_DHS/DHS_',years[yy],'_files_recodes_for_sims.csv'))
    var_index = which(DHS_file_recode_df$variable == 'iptp_doses')
    cur_dta = read.dta(paste0(dta_dir, '/', DHS_file_recode_df$folder_dir[var_index], '/', DHS_file_recode_df$filename[var_index]))
    code_name = DHS_file_recode_df$code[var_index]
    
    # get rid of values that are clearly wrong
    cur_dta[[code_name]][cur_dta[[code_name]]> 10] = NA
    denom = length(which(cur_dta[[code_name]] >=1))
    # only include results if there are 30 or more datapoints
    if(denom>30){
      store_iptp_fractions[, yy] = c(  length(which(cur_dta[[code_name]] ==1))/denom,
                                       length(which(cur_dta[[code_name]] ==2))/denom,
                                       length(which(cur_dta[[code_name]] >=3))/denom)
    } else{
      print(paste0('Fewer than 30 individuals included in sample for year ', years[yy], '. This year excluded from fit.'))
    }
  }
  # assume same dose distribution in last_sim_year as in last observation year
  if(last_sim_year > max(years)){
    store_iptp_fractions = cbind(store_iptp_fractions, store_iptp_fractions[,dim(store_iptp_fractions)[2]])
    colnames_iptp =  c(years, last_sim_year)
  } else{
    colnames_iptp = years
  }
  # label rows and columns of matrix
  colnames(store_iptp_fractions) = colnames_iptp
  rownames(store_iptp_fractions) = c('1 dose', '2 doses', '3 doses')
  
  # remove NA rows
  na_cols = which(is.na(store_iptp_fractions[1,]))
  if(length(na_cols)>1) {
    store_iptp_fractions = store_iptp_fractions[,-na_cols]
    years_included = colnames_iptp[-na_cols]
  } else{
    years_included = colnames_iptp
  }
  
  if(country !='NGA'){
    # smoothed function through these points to get the fraction given each dosage for each year sim_start_year-last_sim_year
    # xx=years_included
    iptp_doses_all_years = matrix(NA, nrow=3, ncol=length(sim_start_year:last_sim_year))
    for(i_dose in 1:dim(store_iptp_fractions)[1]){
      iptp_doses_all_years[i_dose, ] = splinefun(years_included, store_iptp_fractions[i_dose,], method="hyman")(sim_start_year:last_sim_year)
    }
    
    # check result
    if(!all(apply(iptp_doses_all_years, 2, sum)==1)){  # should all be equal to 1, if not, need to divide each column by column sum
      warning('PROBLEM DETECTED: in fraction of individuals receiving 1, 2, or 3 doses. Sum is not 1.')
    }
    # label rows and columns of matrix
    colnames(iptp_doses_all_years) = sim_start_year:last_sim_year
    rownames(iptp_doses_all_years) = c('1 dose', '2 doses', '3 doses')
    
    
    # write results to csv
    if(!is.na(iptp_doses_all_years[1,1])){
      write.csv(iptp_doses_all_years, paste(hbhi_dir,'/simulation_inputs/IPTp/estimated_past_num_doses.csv', sep=''))
      
    } else{
      warning('unknown reason for NAs in smoothed IPTp dose matrix - not writing csv output file.')
    }
  } else{
    # for Nigeria, the fraction of individuals with  three doses goes up and down rather than monotonically increasing (in particular, MIS surveys show better coverage than the DHS surveys). 
    #   For example, a larger fraction of people who received IPTp were recorded as  getting three doses in 2008 than in 2018, which doesn't seem right. I'm not aware of any real-world effects that 
    #   should have caused this (in fact, I would have expected much lower fractions for three doses in 2008, which was  before three doses were even recommended).
    #   I will instead use the average dose distribution reported across all of the years. 
    mean_dose_dist = rowSums(store_iptp_fractions)
    mean_dose_dist = mean_dose_dist / sum(mean_dose_dist)
    iptp_doses_all_years = matrix(NA, nrow=3, ncol=length(sim_start_year:last_sim_year))
    for(i_dose in 1:dim(store_iptp_fractions)[1]){
      iptp_doses_all_years[i_dose, ] = mean_dose_dist[i_dose]
      # iptp_doses_all_years[i_dose, ] = store_iptp_fractions[i_dose,dim(store_iptp_fractions)[2]]  # old version using values from final year instead of average
    }
    # label rows and columns of matrix
    colnames(iptp_doses_all_years) = sim_start_year:last_sim_year
    rownames(iptp_doses_all_years) = c('1 dose', '2 doses', '3 doses')
    write.csv(iptp_doses_all_years, paste(hbhi_dir,'/simulation_inputs/IPTp/estimated_past_num_doses.csv', sep=''))
  }
 
  # # plot the number of IPTp doses reported in different surveys
  # par(mar=c(5,5,4,3))
  # layout(matrix(c(1,1,2), nrow = 1, ncol = 3, byrow = TRUE))
  # barplot(store_iptp_fractions, beside=FALSE, xlab='year', ylab='fraction of individuals receiving dose', col= c(rgb(145/255, 48/255, 88/255, 0.5), rgb(0/255, 160/255, 138/255, 0.5), rgb(83/255, 147/255, 195/255)), cex.lab=1.5, cex.axis=1.5, cex.names=1.5)
  # plot(NA, xlim=c(2008,2018), ylim=c(0,0.6), xlab='', ylab='', axes=FALSE)
  # legend('center', legend=rev(c('1', '2', '>=3')), fill=rev(c(rgb(145/255, 48/255, 88/255, 0.5), rgb(0/255, 160/255, 138/255, 0.5), rgb(83/255, 147/255, 195/255))), title='IPTp doses taken', bty='n', cex=1.8)
  # par(mfrow=c(1,1))
  
  png(paste0(hbhi_dir, '/simulation_inputs/plots/IPTp_barchart_fraction_num_doses_DHSMIS.png'), width=7, height=4, units='in', res=900)
  layout(matrix(c(1,1,2), nrow=1))
  if(last_sim_year > max(years)){
    barplot(store_iptp_fractions[,1:(dim(store_iptp_fractions)[2]-1)], col=(c(rgb(0,0,0,0.75), rgb(0.5,0.5,0.5,0.5), rgb(0.7,0.9, 1, 1))), xlab='year', ylab='IPTp doses', names.arg=years_included[1:(dim(store_iptp_fractions)[2]-1)], cex.axis=1.5, cex.names=1.5, cex.lab=1.5)
  } else{
    barplot(store_iptp_fractions[,1:(dim(store_iptp_fractions)[2])], col=(c(rgb(0,0,0,0.75), rgb(0.5,0.5,0.5,0.5), rgb(0.7,0.9, 1, 1))), xlab='year', ylab='IPTp doses', names.arg=years_included[1:(dim(store_iptp_fractions)[2])], cex.axis=1.5, cex.names=1.5, cex.lab=1.5)
  }
  if(country =='NGA'){
    abline(h=cumsum(mean_dose_dist), col='red')
  }
  plot(NA, ylim=c(0,1), xlim=c(0,1), axes=FALSE, ylab='', xlab='')
  legend(-0.01,0.95, c('>=3 doses', '2 doses', '1 dose'), col=rev(c(rgb(0,0,0,0.75), rgb(0.5,0.5,0.5,0.5), rgb(0.7,0.9, 1, 1))), lwd=5, bty='n', cex=1.5)
  dev.off()
}






