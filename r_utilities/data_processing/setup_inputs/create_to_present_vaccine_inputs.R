# create_to_present_vaccine_inputs.R




#=======================================#
# setup vaccine inputs
#=======================================#
# uses information from the pilot rollout estimated coverages and EPI vaccine coverages to create the vaccine input file
# requires:  pre-generated EPI vaccine estimates from DHS, pilot coverages from early rollout in all admins that should be included in to-present simulations
# creates a csv with the following columns: admin_name, coverage (based on DHS EPI vaccine and pilot introduction), deploy_type (should be 'EPI'), 
#    RTSS_day (day of simulation when vaccinations will be rolled out, accounting for delay from birth-triggered intervention), vaccine ('primary' and 'booster'),	rtss_touchpoints (ages for primary and booster),
#    distribution_name ('CONSTANT_DISTRIBUTION'), distribution_std (1), initial_effect (based on fitting for vaccine efficacy), decay_time_constant (based on fitting for vaccine efficacy), 
#    decay_class ('WaningEffectExponential'), duration (probably -1 assuming it continues to the end of the to-present simulations)

create_vaccine_input_file = function(
    hbhi_dir,
    initial_vacc_coverages,
    vacc_dhs_filepath,
    vacc_rollout_day,
    primary_initial_effect,
    booster_initial_effect,
    decay_time_constant,
    primary_series_day,
    booster_day,
    max_r21_epi_cov,
    booster_relative_coverage = 0.8   # fraction of primary recipients assumed to receive booster
) {
  # ---- Step 1: Process DHS EPI vaccine estimates ----
  # Take minimum of vacc_dpt3 and vacc_measles rates as proxy for R21 uptake.
  # R21 requires 3 doses (like DPT3) and is given at a later age (like measles);
  # the minimum captures both constraints on coverage. Cap at 0.8, floor at 0.01.
  maximum_coverage = 0.8
  var_names = c('vacc_dpt3', 'vacc_measles')
  vacc_dhs = read.csv(vacc_dhs_filepath)
  vacc_dhs$admin_name = vacc_dhs$NOMDEP
  vacc_dhs$coverage = sapply(
    apply(vacc_dhs[, which(colnames(vacc_dhs) %in% paste0(var_names, '_rate'))], 1, min),
    min, maximum_coverage)
  vacc_dhs = vacc_dhs[, c('admin_name', 'coverage')] %>%
    dplyr::mutate(coverage = ifelse(coverage < 0.01, 0.01, coverage)) %>%
    rename(survey_coverage_est = coverage)
  
  # ---- Step 2: Compare pilot and DHS estimates (diagnostic plot) ----
  # Merge DHS estimates into pilot data (all.x keeps all pilot LGAs).
  initial_vacc_coverages = initial_vacc_coverages %>%
    dplyr::select('State', 'LGA', 'method', 'coverage') %>%
    rename(admin_name = LGA, pilot_coverage = coverage) %>%
    merge(vacc_dhs, by = 'admin_name', all.x = TRUE)
  vis_dir <- file.path(hbhi_dir, 'data_visualization', 'R21')
  if (!dir.exists(vis_dir)) dir.create(vis_dir, recursive = TRUE)
  ggplot(initial_vacc_coverages, aes(x = survey_coverage_est, y = pilot_coverage)) +
    geom_point(aes(color = State)) +
    geom_abline(slope = 1, intercept = 0) +
    coord_cartesian(xlim = c(0, 1), ylim = c(0, 1)) +
    ylab('estimated coverage based on R21 vaccine rollout') +
    xlab('coverage estimated from EPI vaccines in DHS survey') +
    theme_bw() +
    facet_wrap('method')
  ggsave(file.path(vis_dir, 'vacc_coverage_compare_dhs24_and_early_implementation.png'),
         width = 10, height = 4)
  
  # ---- Step 3: Average across methods, then average pilot and DHS estimates ----
  # Average the three projection methods (coverage_age, coverage_campaign, coverage_direct)
  # to get one pilot equilibrium coverage estimate per LGA.
  # Only pilot LGAs are retained — admins without pilot data are excluded from the
  # output (they do not receive vaccine in the to-present simulations).
  pilot_mean = initial_vacc_coverages %>%
    group_by(admin_name) %>%
    summarise(
      State               = first(State),
      pilot_coverage      = mean(pilot_coverage, na.rm = TRUE),
      survey_coverage_est = first(survey_coverage_est),
      .groups = 'drop'
    )
  
  # For pilot LGAs with a DHS estimate: use mean of DHS and pilot estimates.
  # For pilot LGAs missing a DHS estimate (e.g. name mismatch): use pilot estimate only.
  vacc_mean_coverages = pilot_mean %>%
    mutate(coverage = ifelse(!is.na(survey_coverage_est),
                             (survey_coverage_est + pilot_coverage) / 2,
                             pilot_coverage)) %>%
    dplyr::select(admin_name, coverage)
  
  # ---- Step 4: Apply maximum coverage cap ----
  # Cap at max_r21_epi_cov based on observed achievable coverage in early rollout.
  vacc_coverage = vacc_mean_coverages %>%
    dplyr::mutate(coverage = ifelse(coverage > max_r21_epi_cov, max_r21_epi_cov, coverage))
  
  # ---- Step 4.5: Visualize coverage assumptions ----
  # Build long-format table with all three estimates per LGA for comparison plots.
  plot_df = pilot_mean %>%
    left_join(vacc_coverage %>% rename(final_coverage = coverage), by = 'admin_name') %>%
    tidyr::pivot_longer(
      cols      = c(survey_coverage_est, pilot_coverage, final_coverage),
      names_to  = 'estimate',
      values_to = 'coverage'
    ) %>%
    mutate(estimate = factor(estimate,
                             levels = c('survey_coverage_est', 'pilot_coverage', 'final_coverage'),
                             labels = c('DHS EPI survey', 'Pilot rollout', 'Final (averaged + capped)')))
  
  state_means = plot_df %>%
    group_by(State, estimate) %>%
    summarise(state_mean = mean(coverage, na.rm = TRUE), .groups = 'drop')
  
  est_colors = c('DHS EPI survey'            = '#1f78b4',
                 'Pilot rollout'             = '#e31a1c',
                 'Final (averaged + capped)' = '#33a02c')
  
  # Plot 1: All three estimates per LGA, faceted by state.
  # Dashed line marks the coverage cap; LGAs on x-axis ordered by final coverage.
  p_lga = ggplot(plot_df,
                 aes(x     = reorder(admin_name, ifelse(estimate == 'Final (averaged + capped)', coverage, NA), mean, na.rm = TRUE),
                     y     = coverage,
                     color = estimate,
                     shape = estimate)) +
    geom_point(size = 2.5, alpha = 0.85, position = position_dodge(0.5)) +
    geom_hline(yintercept = max_r21_epi_cov, linetype = 'dashed', color = 'grey40', linewidth = 0.5) +
    annotate('text', x = Inf, y = max_r21_epi_cov, hjust = 1.05, vjust = -0.5,
             label = paste0('cap = ', scales::percent(max_r21_epi_cov, accuracy = 1)),
             size = 3, color = 'grey40') +
    scale_color_manual(values = est_colors) +
    scale_shape_manual(values = c(16, 17, 15)) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 1), limits = c(0, NA)) +
    facet_wrap(~ State, scales = 'free_x', ncol = 2) +
    labs(title  = 'R21 vaccine coverage assumptions by LGA',
         x      = NULL, y = 'Coverage', color = NULL, shape = NULL) +
    theme_bw(base_size = 10) +
    theme(axis.text.x     = element_text(angle = 45, hjust = 1, size = 7),
          legend.position = 'top',
          strip.text      = element_text(face = 'bold'))
  ggsave(file.path(vis_dir, 'R21_coverage_by_lga_and_estimate.png'), p_lga,
         width = 10, height = 3 + 2.5 * dplyr::n_distinct(plot_df$State),
         units = 'in', dpi = 150, limitsize = FALSE)
  
  # Plot 2: State-level summary — LGA points jittered, state mean as a thick dash.
  # Faceted by estimate type so differences across DHS, pilot, and final are easy to compare.
  p_state = ggplot(plot_df, aes(x = State, y = coverage, color = estimate)) +
    geom_jitter(width = 0.15, alpha = 0.65, size = 2.5) +
    geom_point(data = state_means, aes(y = state_mean),
               shape = 95, size = 10, alpha = 0.9) +
    geom_hline(yintercept = max_r21_epi_cov, linetype = 'dashed', color = 'grey40', linewidth = 0.5) +
    scale_color_manual(values = est_colors) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 1), limits = c(0, NA)) +
    facet_wrap(~ estimate, ncol = 3) +
    labs(title    = 'R21 coverage by state: LGA distribution (dots) and state mean (—)',
         subtitle = paste0('Coverage cap at ', scales::percent(max_r21_epi_cov, accuracy = 1)),
         x        = NULL, y = 'Coverage', color = NULL) +
    theme_bw(base_size = 10) +
    theme(legend.position = 'none',
          axis.text.x     = element_text(angle = 30, hjust = 1),
          strip.text      = element_text(face = 'bold'))
  ggsave(file.path(vis_dir, 'R21_coverage_by_state_summary.png'), p_state,
         width = 10, height = 4, units = 'in', dpi = 150)
  
  # ---- Step 5: Build simulation input dataframe ----
  # Primary series: one row per pilot admin.
  vacc_primary_df = data.frame(
    admin_name          = vacc_coverage$admin_name,
    coverage            = vacc_coverage$coverage,
    deploy_type         = 'EPI',
    RTSS_day            = vacc_rollout_day,
    vaccine             = 'primary',
    rtss_touchpoints    = primary_series_day,
    distribution_name   = 'CONSTANT_DISTRIBUTION',
    distribution_std    = 1,
    initial_effect      = primary_initial_effect,
    decay_time_constant = decay_time_constant,
    decay_class         = 'WaningEffectExponential',
    duration            = -1
  )
  
  # Booster series: coverage = booster_relative_coverage (flat across admins;
  # simulation applies this as conditional on receiving primary series).
  vacc_boost_df = vacc_primary_df %>%
    dplyr::mutate(
      coverage         = booster_relative_coverage,
      vaccine          = 'booster',
      rtss_touchpoints = booster_day,
      initial_effect   = booster_initial_effect
    )
  
  vacc_input_df = rbind(vacc_primary_df, vacc_boost_df)
  
  # ---- Step 6: Save output ----
  out_dir <- file.path(hbhi_dir, 'simulation_inputs', 'interventions_2010_toPresent')
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  write.csv(vacc_input_df,
            file.path(out_dir, 'vacc_2010_toPresent.csv'),
            row.names = FALSE)
  message('  [vaccine] Written ', nrow(vacc_primary_df), ' pilot admins to R21_EPI_vacc_toPresent.csv')
  
  invisible(vacc_input_df)
}
