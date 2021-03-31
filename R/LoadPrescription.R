#' Load in Prescriptions
#'
#' returns information based on the prescription filtered or a list of
#' ids and returns all the people who were given that prescription filtered
#' by, or all the people in the person ids list. There is also an option
#' depending whether you want to filter by the 4th or 5th level of the ATC
#' drug concept, as a broader concept classification may be wanted
#' (e.g. Glucocorticoids vs Salmeterol).
#'
#' @param project_name string
#' @param data_base_name string
#' @param prescription integer
#' @param person vector
#' @param ATC_4th boolean
#'
#' @return
#' @export
#'
#' @examples
#' LoadPrescription("yhcr-prd-phm-bia-core", "CY_CDM_V1_50k_Random",
#'                   prescription = 21603256, person = NULL,  ATC_4th = FALSE)
LoadPrescription <- function(project_name, data_base_name,
                             prescription = NULL,
                             person = NULL,  ATC_4th = FALSE) {
  if (ATC_4th == FALSE) {
    sql <- paste("Select distinct A.drug_exposure_id, A.person_id, A.drug_concept_id, lol.concept_name, A.drug_exposure_start_date, A.drug_exposure_end_date,
      A.drug_type_concept_id, A.sig, A.visit_occurrence_id, A.dose_unit_source_value,
      CA2.ancestor_CONCEPT_ID, C2.concept_name as ancestor_concept_name, lol.vocabulary_id, lol.concept_class_id, lol.standard_concept,
      lol.concept_code, lol.valid_end_date
      FROM", paste(project_name, data_base_name, "drug_exposure", sep = "."), "A JOIN",
                 paste(project_name, data_base_name, "concept", sep = "."), "lol ON A.DRUG_CONCEPT_ID = lol.concept_id
      JOIN", paste(project_name, data_base_name, "concept_ancestor", sep = "."), "CA2
      ON CA2.descendant_CONCEPT_ID = A.DRUG_CONCEPT_ID JOIN",
                 paste(project_name, data_base_name, "concept", sep = "."),
                 "C2 ON CA2.ancestor_CONCEPT_ID = C2.concept_id
      and c2.vocabulary_id = 'ATC'
      and c2.concept_class_id = 'ATC 5th'
      and c2.invalid_reason is null", sep = " ")
    if (!is.null(prescription) | !is.null(person) ) {
      sql <- paste(sql, "WHERE", sep=" ")
    }
    if (!is.null(prescription)) {
      sql <- paste(sql, "CA2.ancestor_concept_id =", toString(prescription), sep=" ")
    }
  }
  else if (ATC_4th == TRUE) {
    sql <- "select distinct A.drug_exposure_id, A.person_id, A.drug_concept_id, c_drug.concept_name, A.drug_exposure_start_date, A.drug_exposure_end_date,
    A.drug_type_concept_id, A.sig, A.visit_occurrence_id, A.dose_unit_source_value,
    CA2.concept_id as ancestor_concept_name_id, CA2.concept_name as ancestor_concept_name, c_drug.vocabulary_id, c_drug.concept_class_id, c_drug.standard_concept,
    c_drug.concept_code, c_drug.valid_end_date  from `yhcr-prd-phm-bia-core.CY_CDM_V1_50k_Random.concept` CA2
    inner join `yhcr-prd-phm-bia-core.CY_CDM_V1_50k_Random.concept_relationship` cr_atc5th
    on CA2.concept_id = cr_atc5th.concept_id_1 and cr_atc5th.relationship_id = 'Subsumes' and cr_atc5th.invalid_reason is null
    inner join `yhcr-prd-phm-bia-core.CY_CDM_V1_50k_Random.concept_relationship` cr_rxnorm
    on cr_atc5th.concept_id_2 = cr_rxnorm.concept_id_1 and cr_rxnorm.relationship_id = 'Maps to' and cr_rxnorm.invalid_reason is null -- gets the RxNorm Ingredient
    inner join `yhcr-prd-phm-bia-core.CY_CDM_V1_50k_Random.concept_ancestor` ca
    on cr_rxnorm.concept_id_2 = ca.ancestor_concept_id
    inner join `yhcr-prd-phm-bia-core.CY_CDM_V1_50k_Random.drug_exposure` A
    on ca.descendant_concept_id = A.drug_concept_id
    inner join `yhcr-prd-phm-bia-core.CY_CDM_V1_50k_Random.concept` c_drug on ca.descendant_concept_id = c_drug.concept_id"
    if (!is.null(prescription) | !is.null(person) ) {
      sql <- paste(sql, "WHERE", sep=" ")
    }
    if (!is.null(prescription)) {
      sql <- paste(sql, "CA2.concept_id =", toString(prescription), sep=" ")
    }
  }


  if (!is.null(person) & !is.null(prescription)) {
    test <- as.character(person)
    test <- paste(test, sep = '', collapse = ' or person_id = ')
    test1 <- paste("AND person_id = ", test)
    sql <- paste(sql, test1, sep=" ")
  }
  else if (!is.null(person) & is.null(prescription)) {
    test <- as.character(person)
    test <- paste(test, sep = '', collapse = ' or person_id = ')
    test1 <- paste("person_id = ", test)
    sql <- paste(sql, test1, sep=" ")
  }

  tb <- bigrquery::bq_project_query(project_name, sql)
  return(bigrquery::bq_table_download(tb))
}
