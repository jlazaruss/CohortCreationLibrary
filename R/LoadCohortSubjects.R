LoadCohortSubjects <- function(project_name, data_base_name, condition_concept = NULL,
                               start_date = NULL, end_date = NULL)          {
  sql <- paste("SELECT con.condition_occurrence_id, con.person_id, con.condition_concept_id, lol.concept_name, con.condition_start_date, con.condition_end_date,
                con.condition_type_concept_id, con.condition_status_concept_id, con.visit_occurrence_id, con.condition_source_value,
                cast(CONCAT(per.year_of_birth,'-', per.month_of_birth,'-',per.day_of_birth) as string) as Birthday, per.death_datetime,
                per.gender_source_value, per.race_source_value, CAST(V.visit_start_datetime AS STRING) visit_start_datetime,
                CAST(V.visit_end_datetime AS STRING) visit_end_datetime, V.visit_concept_id, V.visit_type_concept_id,
                V.care_site_id, V.visit_source_concept_id FROM",
               paste(project_name, data_base_name, "condition_occurrence", sep = "."),
               "con INNER JOIN", paste(project_name, data_base_name, "person", sep = "."),
               "per ON con.person_id = per.person_id",sep=" ")
  sql <- paste(sql ,"JOIN", paste(project_name, data_base_name, "concept", sep = "."),
               "lol ON con.condition_concept_id = lol.concept_id",
               "LEFT JOIN", paste(project_name, data_base_name, "visit_occurrence", sep = "."),
               "V ON con.visit_occurrence_id = V.visit_occurrence_id",sep = " " )
  if (!is.null(condition_concept)) {
    sql <- paste(sql, "WHERE con.condition_concept_id =", toString(condition_concept),sep = " ")
  }
  if (!is.null(start_date)){
    sql <- paste(sql, "AND  condition_start_date >=",
                 paste("'",toString(start_date),"'", sep = "") ,
                 sep = " ")
  } #Filtering by date
  if (!is.null(end_date)){
    sql <- paste(sql, "AND  condition_end_date <=",
                 paste("'",toString(end_date),"'", sep = ""),
                 sep = " ")
  } #Filtering by date
  print(sql)
  return(bigrquery::bq_table_download(bigrquery::bq_project_query(project_name, sql)))
}
