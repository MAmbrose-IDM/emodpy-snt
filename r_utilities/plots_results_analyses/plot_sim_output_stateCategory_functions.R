

######################################################################
# create plot panel with all burden metrics, no intervention info (either showing burden or burden relative to burden in specified year)
######################################################################

plot_simulation_output_burden_all = function(sim_future_output_dir, pop_filepath, list_district_subset, list_cur_admins, 
                                             plot_by_month, min_year, max_year, sim_end_years, relative_year=NA,
                                             pyr='', chw_cov='',
                                             scenario_filepaths, scenario_names, experiment_names, scenario_palette, LLIN2y_flag=FALSE, overwrite_files=FALSE, 
                                             separate_plots_flag=FALSE, extend_past_timeseries_year=NA, scenario_linetypes=NA, plot_CI=TRUE, include_U1=FALSE,
                                             burden_metric_subset=c()){
  
  if (!is.na(relative_year)){ if(relative_year<min_year){
    warning('specified minimum year must be <= relative year. Setting min_year to relative_year.')
    min_year = relative_year
  }}
  
  ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ###
  # combine simulation output from multiple scenarios
  ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ###
  pop_sizes = read.csv(pop_filepath)
  pop_sizes = pop_sizes[,c('admin_name','pop_size')]
  # if we include all admins, get list of names from population size dataframe
  if(cur_admins[1] == 'all'){
    cur_admins = unique(pop_sizes$admin_name)
  }
  
  # create output directories
  if(!dir.exists(paste0(sim_future_output_dir, '/_plots'))) dir.create(paste0(sim_future_output_dir, '/_plots'))
  if(!dir.exists(paste0(sim_future_output_dir, '/_plots/timeseries_dfs'))) dir.create(paste0(sim_future_output_dir, '/_plots/timeseries_dfs'))
  if(plot_by_month){
    time_string = 'monthly'
  } else time_string = 'annual'
  
  
  # ----- malaria burden ----- #
  burden_metrics = c('PfPR', 'PfPR', 'incidence', 'incidence', 'directMortality', 'directMortality', 'allMortality', 'allMortality')#, 'mLBW_deaths', 'MiP_stillbirths')
  burden_metric_names = c('PfPR (U5)', 'PfPR (all ages)', 'incidence (U5)', 'incidence (all ages)', 'direct mortality (U5)', 'direct mortality (all ages)', 'mortality (U5)', 'mortality (all ages)')#, 'mLBW mortality (births)', 'stillbirths (births)')
  burden_colnames = c('PfPR_U5', 'PfPR_MiP_adjusted', 'New_clinical_cases_U5', 'New_Clinical_Cases', 'direct_mortality_nonMiP_U5_mean', 'direct_mortality_nonMiP_mean', 'total_mortality_U5_mean', 'total_mortality_mean')#, 'mLBW_deaths', 'MiP_stillbirths')    
  if(include_U1){
    burden_metrics = c('PfPR', 'PfPR', 'PfPR', 'incidence','incidence', 'incidence', 'directMortality', 'directMortality', 'directMortality', 'allMortality', 'allMortality', 'allMortality')#, 'mLBW_deaths', 'MiP_stillbirths')
    burden_metric_names = c('PfPR (U1)', 'PfPR (U5)', 'PfPR (all ages)', 'incidence (U1)', 'incidence (U5)', 'incidence (all ages)', 'direct mortality (U1)', 'direct mortality (U5)', 'direct mortality (all ages)', 'mortality (U1)', 'mortality (U5)', 'mortality (all ages)')#, 'mLBW mortality (births)', 'stillbirths (births)')
    burden_colnames = c('PfPR_U1', 'PfPR_U5', 'PfPR_MiP_adjusted', 'New_clinical_cases_U1', 'New_clinical_cases_U5', 'New_Clinical_Cases', 'direct_mortality_nonMiP_U1_mean', 'direct_mortality_nonMiP_U5_mean', 'direct_mortality_nonMiP_mean', 'total_mortality_U1_mean', 'total_mortality_U5_mean', 'total_mortality_mean')#, 'mLBW_deaths', 'MiP_stillbirths')
  }
  # allow subsetting of which burden metrics plotted (based on burden_metric_subset argument)
  if((length(burden_metric_subset)>=1)){
    burden_metrics_subset_indices = which(burden_metrics %in% burden_metric_subset)
    burden_colnames = burden_colnames[burden_metrics_subset_indices]
    burden_metric_names = burden_metric_names[burden_metrics_subset_indices]
    burden_metrics = burden_metrics[burden_metrics_subset_indices]
  }
  
  gg_list = list()
  for(bb in 1:length(burden_colnames)){
    burden_metric_name = burden_metric_names[bb]
    burden_colname = burden_colnames[bb]
    burden_metric = burden_metrics[bb]
    
    if(grepl('U1', burden_metric_name)){
      age_plotted = 'U1'
    } else if(grepl('U5', burden_metric_name)){
      age_plotted = 'U5'
    } else if(grepl('births', burden_metric_name)){
      age_plotted = 'births'
    } else age_plotted = 'all'
    
    
    # check whether burden output already exists for this comparison
    if(LLIN2y_flag){
      llin2y_string = '_2yLLIN'
    } else{
      llin2y_string = ''
    }
    
    # iterate through scenarios, storing relevant output
    burden_df = data.frame()
    for(ee in 1:length(scenario_filepaths)){
      cur_sim_output_agg = get_burden_timeseries_exp(exp_filepath = scenario_filepaths[ee],
                                                     exp_name = scenario_names[ee], district_subset=district_subset,
                                                     cur_admins=cur_admins, pop_sizes = pop_sizes, min_year=min_year, max_year=max_year, burden_colname=burden_colname, age_plotted=age_plotted, plot_by_month=plot_by_month)
      if(nrow(burden_df)==0){
        burden_df = cur_sim_output_agg
      } else{
        burden_df = rbind(burden_df, cur_sim_output_agg)
      }
    }
    
    ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ###
    # Connect the 'to-present' and 'future-projection' simulations in the plot
    ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ###
    # Two alternatives for how this is done, controlled by extend_past_timeseries:
    #   - (FALSE) extend the 'future-projection' lines all back to the end of the 'to-present' simulations, which is desirable if the future projection scenarios separate right away
    #   - (TRUE) extend the end of the 'to-present' line up to the specified point in the 'future-projections' timeseries. This is only desirable if all 'future-projections' are 
    #            identical up to that point (e.g., 'to-present' simulations only run to 2020 and we are currently in 2023, so 2021-2022 are identical in all 'future projection' scenarios)
    if('to-present' %in% burden_df$scenario){
      connect_future_with_past = TRUE
      similarity_threshold = 0.15
      if(!is.na(extend_past_timeseries_year) & (extend_past_timeseries_year %in% burden_df$year[burden_df$scenario != 'to-present']) & time_string=='annual'){
        # check whether the future projections are all nearly identical (minus stochasticity) for the initial year (otherwise, use version that extends future-projection lines back to past)
        future_df = burden_df[burden_df$scenario != 'to-present',]
        earliest_future_year = min(future_df$year)
        compare_burdens = future_df$mean_burden[future_df$year == earliest_future_year]
        if(all(compare_burdens<(compare_burdens[1]*(1+similarity_threshold))) & all(compare_burdens>(compare_burdens[1]*(1-similarity_threshold)))){
          connect_future_with_past = FALSE
          merge_years = earliest_future_year
          if(extend_past_timeseries_year > earliest_future_year){
            # check which years (up to a maximum of extend_past_timeseries_year) should be included in the to-present line
            yy = earliest_future_year + 1
            while(yy <= extend_past_timeseries_year){
              compare_burdens = future_df$mean_burden[future_df$year == (yy)]
              if(all(compare_burdens<(compare_burdens[1]*1.05)) & all(compare_burdens>(compare_burdens[1]*0.95))){
                merge_years = c(merge_years, yy)
                yy = yy+1
              } else{  # as soon as they don't match for a year, stop trying to match any future years
                yy=99999999
              }
            }
          }
          # get the mean value from the 'future-projection' rows so that it can be added to the 'to-present' scenario
          past_from_future_df = future_df[future_df$year %in% merge_years,]
          past_from_future_df_means = past_from_future_df %>% dplyr::select(-scenario) %>% group_by(year) %>%
            summarise_all(mean) %>% ungroup()
          past_from_future_df_means$scenario = 'to-present'
          # delete the old 'future-projection' rows for all but the final of these years
          delete_future_years = merge_years[merge_years != max(merge_years)]
          if(length(delete_future_years)>0) burden_df = burden_df[-which(burden_df$year %in% merge_years),]
          # add the rows to the 'to-present' scenario in the data frame
          burden_df = merge(burden_df, past_from_future_df_means, all=TRUE)
        }else{
          connect_future_with_past = TRUE
        }
      } 
      if(connect_future_with_past){
        # add the final 'to-present' row to all future simulations for a continuous plot
        to_present_df = burden_df[burden_df$scenario == 'to-present',]
        if(plot_by_month){
          final_to_present_row = to_present_df[as.Date(to_present_df$date) == max(as.Date(to_present_df$date)),]
          for(ss in 2:length(scenario_names)){
            final_to_present_row$scenario = scenario_names[ss]
            burden_df = rbind(burden_df, final_to_present_row)
          }
        } else{
          final_to_present_row = to_present_df[to_present_df$year == max(to_present_df$year),]
          for(ss in 2:length(scenario_names)){
            final_to_present_row$scenario = scenario_names[ss]
            burden_df = rbind(burden_df, final_to_present_row)
          }
        }
      }
    }
    
    
    
    
    ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ###
    # if plotting burden relative to specified year, calculate relative values
    ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ###
    if (!is.na(relative_year) & time_string=='annual'){
      # if the reference year is in the to-present simulation, use the same reference for all scenarios
      # if the reference year is not in the to-present simulation, use average value across scenarios and check that all scenarios have similar values for the reference year (if they do not, send a warning)
      if(('to-present' %in% burden_df$scenario) & (relative_year %in% unique(burden_df$year[burden_df$scenario=='to-present']))){
        # get the burden in the reference year
        reference_burden_cur = burden_df$mean_burden[burden_df$scenario=='to-present' & burden_df$year == relative_year]
      } else{
        similarity_threshold = 0.1
        all_ref_year_burdens = burden_df$mean_burden[burden_df$year == relative_year]
        reference_burden_cur = mean(all_ref_year_burdens)
        if(any(all_ref_year_burdens>(reference_burden_cur*(1+similarity_threshold))) | any(all_ref_year_burdens<(reference_burden_cur*(1-similarity_threshold)))){
          warning(paste0('in the reference year, some scenarios have different burdens for ',burden_metric_name))
        }
      }
      # calculate all relative burden values as mean_burden / reference_burden_cur: this will be referred to as 'burden relative to burden in relative_year'
      burden_df$mean_burden = burden_df$mean_burden / reference_burden_cur
      burden_df$max_burden = NA
      burden_df$min_burden = NA
      
      ylab_add_component = paste0('\n relative to ', relative_year)
      relative_string = paste0('_relativeTo', relative_year)
    } else{
      ylab_add_component = ''
      relative_string = ''
    }
    
    
    ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ###
    # create scenario-comparison plots
    ### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ###
    
    # subset to relevant scenarios currently being compared
    burden_df = burden_df[burden_df$scenario %in% scenario_names,]
    # get factors in the correct order (rather than alphabetical)
    burden_df$scenario = factor(burden_df$scenario, levels=rev(scenario_names))
    
    if(is.na(scenario_linetypes[1])){
      scenario_linetypes = rep(1, length(unique(burden_df$scenario)))
      names(scenario_linetypes) = unique(burden_df$scenario)
    }
    # ----- malaria burden ----- #
    
    if(plot_by_month){
      gg_list[[bb]] = ggplot(burden_df, aes(x=as.Date(date), y=mean_burden, color=scenario)) +
        geom_ribbon(aes(ymin=min_burden, ymax=max_burden, fill=scenario), alpha=0.1, color=NA)+
        scale_fill_manual(values = rev(scenario_palette)) + 
        geom_line(linewidth=1) + 
        scale_color_manual(values = rev(scenario_palette)) + 
        xlab('date') + 
        ylab(paste0(gsub('\\(births\\)', '', burden_metric_name),ylab_add_component)) + 
        xlim(as.Date(paste0(min_year, '-01-01')), as.Date(paste0(max_year, '-01-01'))) +
        coord_cartesian(ylim=c(0, NA)) +
        theme_classic()+ 
        theme(legend.position = "top", legend.box='horizontal', legend.title = element_blank(), legend.text=element_text(size = text_size))
    } else{
      gg_list[[bb]] = ggplot(burden_df, aes(x=year, y=mean_burden, color=scenario, linetype=scenario))  +
        geom_line(linewidth=1) + 
        scale_linetype_manual(values=rev(scenario_linetypes)) +
        scale_color_manual(values = rev(scenario_palette)) + 
        xlab('year') + 
        ylab(paste0(gsub('\\(births\\)', '', burden_metric_name), ylab_add_component)) + 
        xlim(min_year, max_year) + 
        scale_x_continuous(breaks= pretty_breaks()) +
        coord_cartesian(ylim=c(0, NA)) +
        theme_classic()+ 
        theme(legend.position = "top", legend.box='horizontal', legend.title = element_blank(), legend.text=element_text(size = text_size))
    }
    if(plot_CI){
      gg_list[[bb]] =  gg_list[[bb]] +
        geom_ribbon(aes(ymin=min_burden, ymax=max_burden, fill=scenario), alpha=0.1, color=NA)+
        scale_fill_manual(values = scenario_palette)
    }
    if(separate_plots_flag){
      separate_plot = gg_list[[bb]] + theme(legend.position='none', text=element_text(size =separate_plot_text_size))
      ggsave(paste0(sim_future_output_dir, '/_plots/',time_string,'Timeseries_', relative_string, burden_metric_name,'_',district_subset,'.png'), separate_plot, dpi=600, width=4, height=3, units='in')  # width=4*0.9, height=3*0.7, # width=4*0.7, height=3*1.2,
    }
  }
  # gg_list = append(list(ggpubr::as_ggplot(ggpubr::get_legend(gg_list[[1]])), (ggplot() + theme_void())), gg_list)
  gg_list = append(list(ggpubr::as_ggplot(ggpubr::get_legend(gg_list[[1]]))), gg_list)
  # remove legend from main plots
  for(bb in 2:(length(burden_colnames)+1)){
    gg_list[[bb]] = gg_list[[bb]] + theme(legend.position = "none")  + theme(text = element_text(size = text_size))   
  }
  # ----- combine all burden plots ----- #
  # gg = grid.arrange(grobs = gg_list, layout_matrix = matrix(c(1,1,2:(length(burden_colnames)+1)), ncol=2, byrow=TRUE))  # other orientation
  nrow_plot = 2
  if(include_U1) nrow_plot = 3
  num_in_matrix = ceiling(length(burden_colnames)/nrow_plot)*nrow_plot
  gg = grid.arrange(grobs = gg_list, layout_matrix = rbind(matrix(rep(1, ceiling(length(burden_colnames)/nrow_plot)), nrow=1), matrix(2:(num_in_matrix+1), nrow=nrow_plot, byrow=FALSE)))
  
  if(save_plots){
    ggsave(paste0(sim_future_output_dir, '/_plots/',time_string,'Timeseries_burden', relative_string,'_pyr', pyr, '_', chw_cov, 'CHW_',district_subset,'.png'), gg, dpi=600, width=9, height=3*nrow_plot, units='in')
  }
  
  return(gg)
}


