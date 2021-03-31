#' Perform Query
#'
#' Carries out Query on the OMOP CDM
#'
#' @param project_name string
#' @param sql string
#'
#' @return data frame
#' @export
#'
#' @examples
#' project_name <- "yhcr-prd-phm-bia-core"
#' sql <- "SELECT *,
#'         cast(CONCAT(year_of_birth,'-',month_of_birth,'-',day_of_birth) as string)
#'         as Birthday FROM yhcr-prd-phm-bia-core.CY_CDM_V1_50k_Random.person
#'         LIMIT 1000"
#' person <- QueryOmop(project_name, sql)
QueryOmop <- function(project_name, sql) {
  tb <- bigrquery::bq_project_query(project_name, sql)
  df <- bigrquery::bq_table_download(tb)
  return(df)

}
