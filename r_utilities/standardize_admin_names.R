# standardize_admin_names.R
# November 2022
# contact: mambrose
#
# Goal: the names of LGAs are often different across different files and we want to make sure that they are used consistently
#    This function takes a 'target' naming system and a 'origin' set of names and matches the origin LGA names with the target names.
#    It then updates the origin names so that the same LGA names can be used consistently across all files.
# NOTE OF CAUTION: some NGA admin2 names appear in multiple admin1s, so they must be renamed to end in a 1 or 2, depending on the admin1. Currently done manually. Includes:
# 'BASSA'
# 'IFELODUN'
# 'IREPODUN'
# 'NASARAWA'
# 'OBI'
# 'SURULERE'
library(stringr)

# if there are admin names that shared across multiple admins (without their numeric distintuishers) in the inputs, will need to have user modify
duplicate_names = c('BASSA', 'IFELODUN', 'IREPODUN', 'NASARAWA', 'OBI', 'SURULERE')  # LGA
duplicate_names_state = c()

create_reference_name_match = function(lga_name){
  #' alter string lga_name to standardized admin2 (LGA) format, also check for and fix common misspellings
  #' @param lga_name the name of the admin2 that should be changed to standardized format
  
  lga_name = str_replace_all(lga_name, pattern=" State", replacement='')
  lga_name = str_replace_all(lga_name, pattern=" state", replacement='')
  lga_name = str_replace_all(lga_name, pattern=' ', replacement='-')
  lga_name = str_replace_all(lga_name, pattern='/', replacement='-')
  lga_name = str_replace_all(lga_name, pattern='_', replacement='-')
  lga_name = str_replace_all(lga_name, pattern="'", replacement='')
  lga_name = toupper(lga_name)
  
  # first value (the name) is the one that is replaced by the second value
  replace_list = list(
                      # NGA 
                      # 'IFELODUN' = 'IFELODUN1',
                      # 'IREPODUN' = 'IREPODUN1',
                      # 'BASSA' = 'BASSA1',
                      # 'NASARAWA' = 'NASARAWA2',
                      # 'SURULERE' = 'SURULERE2',
                      # 'OBI' = 'OBI2',
                      'GANYE' = 'GANAYE',
                      'GIREI' = 'GIRERI',
                      'DAMBAM' = 'DAMBAN',
                      'MARKURDI' = 'MAKURDI',
                      'MUNICIPAL-AREA-COUNCIL' = 'ABUJA-MUNICIPAL',
                      'SHONGOM' = 'SHOMGOM',
                      # 'BIRNIN' = 'BIRINIWA', 'BIRNI-KUDU'?
                      'BIRNIN-KUDU' = 'BIRNI-KUDU',
                      'BIRNIWA' = 'BIRINIWA',
                      'KIRI-KASAMA' = 'KIRI-KASAMMA',
                      'MALAM-MADURI' = 'MALAM-MADORI',
                      'SULE-TANKAKAR' = 'SULE-TANKARKAR',
                      'MAKARFI' = 'MARKAFI',
                      'ZANGON-KATAF' = 'ZANGO-KATAF',
                      'DANBATTA' = 'DAMBATTA',
                      'TAKAI' = 'TAKALI',
                      'UNGONGO' = 'UNGOGO',
                      'DAN' = 'DAN-MUSA',
                      'DUTSIN' = 'DUTSIN-MA',
                      'MATAZU' = 'MATAZUU',
                      'PATIGI' = 'PATEGI',
                      'MUNYA' = 'MUYA',
                      'PAIKORO' = 'PAILORO',
                      'BARKIN-LADI' = 'BARIKIN-LADI',
                      'DANGE-SHUNI' = 'DANGE-SHNSI',
                      'GWADABAWA' = 'GAWABAWA',
                      'WAMAKKO' = 'WAMAKO',
                      'ARDO' = 'ARDO-KOLA',
                      'KARIM' = 'KURMI',
                      'BARDE' = 'BADE',
                      'BORSARI' = 'BURSARI',
                      'TARMUWA' = 'TARMUA',
                      # BDI
                      'BUJUMBURA-CENTRE' = 'ZONE-CENTRE',
                      'BUJUMBURA-NORD' = 'ZONE-NORD',
                      'BUJUMBURA-SUD' = 'ZONE-SUD',
                      # 'Bukinanyana' = 'Cibitoke',  # Bukinanyana is a commune in Cibitoke province - may now be a new DS
                      # 'Gisuru' = 'Ruyigi',  # Gisuru is a commune in Ruyigi province - may now be a new DS
                      # 'Rutovu' = 'Bururi'  # Rutovu is a commune in Bururi province - may now be a new DS
                      
                      # state names
                      'AKWA-LBOM' = 'AKWA-IBOM',
                      'FEDERAL-CAPITAL-TERRITORY' = 'FCT-ABUJA',
                      'FCT' = 'FCT-ABUJA',
                      'CROSS-RIVER' = 'CRORIVER'

  )
  if(lga_name %in% names(replace_list)){
    lga_name = replace_list[lga_name][[1]]
  }
  return(lga_name)
}

standardize_admin_names_in_df = function(target_names_df, origin_names_df, target_names_col='admin_name', origin_names_col='admin_name'){
  #' given a dataframe with a column containing admin names, update the admin names to match the standardized naming conventions
  #' @param target_names_df dataframe containing a column with the admin names that should be used as the standard
  #' @param origin_names_df dataframe containing a column with admin names that user would like to update to the standard
  #' @param target_names_col name of column in target_names_df where the standard admin names are found
  #' @param origin_names_col name of column in origin_names_df where the to-be-updated admin names are found
  #' @return the origin_names_df data frame, with the admin names replaced with the standardized version
  
  if(any(duplicate_names %in% toupper(origin_names_df[[origin_names_col]]))){
    stop('PROBLEM ENCOUNTERED: Some of the input admin names are not unique identifiers of the admin. Need to add numeric value to distintuish, determined by admin region, following the system from the standardization base file.')
  } else{
    
    target_names_df$matched_name = sapply(target_names_df[[target_names_col]], create_reference_name_match)
    origin_names_df$matched_name = sapply(origin_names_df[[origin_names_col]], create_reference_name_match)
    
    # check whether all names from the origin source are now matched to one of the target names
    if(!all(origin_names_df$matched_name %in% target_names_df$matched_name)){
      warning('Some of the source admin names could not be matched with a target admin name')
      View(distinct(origin_names_df[which(!(origin_names_df$matched_name %in% target_names_df$matched_name)), c('matched_name', 'State')]))
      View(target_names_df[,c('matched_name', 'State')])
    } 
    if('data.table' %in% class(origin_names_df)){  # changes how indexing works for columns in dataframe
      origin_names_df = as.data.frame(origin_names_df)
      was_data_table=TRUE
    } else was_data_table = FALSE
    if('data.table' %in% class(target_names_df)){  
      target_names_df = as.data.frame(target_names_df)
    }
    # remove the original admin name column from the origin dataframe, and also the target name column if applicable (for merging)
    origin_names_df = origin_names_df[,-(which(colnames(origin_names_df)==origin_names_col))]
    if(target_names_col %in% colnames(origin_names_df)){
      origin_names_df = origin_names_df[,-which(colnames(origin_names_df)==target_names_col)]
    }
    
    # add the updated admin names to the dataframe, under the original admin column name
    target_names_df =  target_names_df[,c('matched_name', target_names_col)]
    updated_names_df = merge(origin_names_df, target_names_df, all.x=TRUE, by='matched_name')
    colnames(updated_names_df)[colnames(updated_names_df)==target_names_col] = origin_names_col
    
    if(was_data_table) updated_names_df = as.data.table(updated_names_df)
    return(updated_names_df)
  }
}



standardize_state_names_in_df = function(target_names_df, origin_names_df, target_names_col='admin_name', origin_names_col='admin_name'){
  #' given a dataframe with a column containing admin names, update the admin names to match the standardized naming conventions
  #' @param target_names_df dataframe containing a column with the admin names that should be used as the standard
  #' @param origin_names_df dataframe containing a column with admin names that user would like to update to the standard
  #' @param target_names_col name of column in target_names_df where the standard admin names are found
  #' @param origin_names_col name of column in origin_names_df where the to-be-updated admin names are found
  #' @return the origin_names_df data frame, with the admin names replaced with the standardized version
  
  if(any(duplicate_names_state %in% toupper(origin_names_df[[origin_names_col]]))){
    stop('PROBLEM ENCOUNTERED: Some of the input admin names are not unique identifiers of the admin. Need to add numeric value to distintuish, determined by admin region, following the system from the standardization base file.')
  } else{
    
    target_names_df$matched_name = sapply(target_names_df[[target_names_col]], create_reference_name_match)
    origin_names_df$matched_name = sapply(origin_names_df[[origin_names_col]], create_reference_name_match)
    
    # check whether all names from the origin source are now matched to one of the target names
    if(!all(origin_names_df$matched_name %in% target_names_df$matched_name)){
      warning('Some of the source admin names could not be matched with a target admin name')
      View(distinct(origin_names_df[which(!(origin_names_df$matched_name %in% target_names_df$matched_name)), c('matched_name', 'State')]))
      View(target_names_df[,c('matched_name', 'State')])
    } 
    if('data.table' %in% class(origin_names_df)){  # changes how indexing works for columns in dataframe
      origin_names_df = as.data.frame(origin_names_df)
      was_data_table=TRUE
    } else was_data_table = FALSE
    if('data.table' %in% class(target_names_df)){  
      target_names_df = as.data.frame(target_names_df)
    }
    # remove the original admin name column from the origin dataframe, and also the target name column if applicable (for merging)
    origin_names_df = origin_names_df[,-(which(colnames(origin_names_df)==origin_names_col))]
    if(target_names_col %in% colnames(origin_names_df)){
      origin_names_df = origin_names_df[,-which(colnames(origin_names_df)==target_names_col)]
    }
    
    # add the updated admin names to the dataframe, under the original admin column name
    target_names_df =  target_names_df[,c('matched_name', target_names_col)]
    updated_names_df = merge(origin_names_df, target_names_df, all.x=TRUE, by='matched_name')
    colnames(updated_names_df)[colnames(updated_names_df)==target_names_col] = origin_names_col
    
    if(was_data_table) updated_names_df = as.data.table(updated_names_df)
    return(updated_names_df)
  }
}





standardize_admin_names_in_vector = function(target_names, origin_names){
  #' given a vector of admin names, update the admin names to match the standardized naming conventions provided in target_names
  #' @param target_names set of admin names that should be used as the standard
  #' @param origin_names current set of admin names that user would like to update to the standard
  #' @return a vector with updated admin names, in the same order as origin_names
  
  
  if(any(duplicate_names %in% toupper(origin_names))){
    stop('PROBLEM ENCOUNTERED: Some of the input admin names are not unique identifiers of the admin. Need to add numeric value to distintuish, determined by admin region, following the system from the standardization base file.')
  } else{
    target_matched_name = sapply(target_names, create_reference_name_match)
    origin_matched_name = sapply(origin_names, create_reference_name_match)
    
    # check whether all names from the origin source are now matched to one of the target names
    if(!all(origin_matched_name %in% target_matched_name)){
      warning('Some of the source admin names could not be matched with a target admin name')
    } 
    
    # create dataframes to merge
    target_df = data.frame('matched_name'=target_matched_name, 'target_name'=target_names)
    origin_df = data.frame('matched_name'=origin_matched_name, 'original_name'=origin_names)
  
    # add the updated admin names to the dataframe, under the original admin column name
    updated_names_df = merge(origin_df, target_df, all.x=TRUE, by='matched_name', sort=FALSE)
    if(all(updated_names_df$original_name == origin_names)){
      return(updated_names_df$target_name)
    } else{
      warning('merge did not maintain original order, need to fix standardize_admin_names_in_vector function or use standardize_admin_names_in_df')
      return(NA)
    }
  }
}
