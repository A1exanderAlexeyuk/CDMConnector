# Copyright 2024 DARWIN EU®
#
# This file is part of CDMConnector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.




#' Create a new generated cohort set from a list of concept sets
#'
#' @description
#'
#' Generate a new cohort set from one or more concept sets. Each
#' concept set will result in one cohort and represent the time during which
#' the concept was observed for each subject/person. Concept sets can be
#' passed to this function as:
#' \itemize{
#'  \item{A named list of numeric vectors, one vector per concept set}
#'  \item{A named list of Capr concept sets}
#' }
#'
#' Clinical observation records will be looked up in the respective domain tables
#' using the vocabulary in the CDM. If a required domain table does not exist in
#' the cdm object a warning will be given.
#' Concepts that are not in the vocabulary or in the data will be silently ignored.
#' If end dates are missing or do not exist, as in the case of the procedure and
#' observation domains, the the start date will be used as the end date.
#'
#' @param cdm A cdm reference object created by `CDMConnector::cdmFromCon` or `CDMConnector::cdm_from_con`
#' @param conceptSet,concept_set A named list of numeric vectors or a Concept Set Expression created
#' `omopgenerics::newConceptSetExpression`
#' @param name The name of the new generated cohort table as a character string
#' @param limit Include "first" (default) or "all" occurrences of events in the cohort
#' \itemize{
#'  \item{"first" will include only the first occurrence of any event in the concept set in the cohort.}
#'  \item{"all" will include all occurrences of the events defined by the concept set in the cohort.}
#' }
#' @param requiredObservation,required_observation A numeric vector of length 2 that specifies the number of days of
#' required observation time prior to index and post index for an event to be included in the cohort.
#' @param end How should the `cohort_end_date` be defined?
#' \itemize{
#'  \item{"observation_period_end_date" (default): The earliest observation_period_end_date after the event start date}
#'  \item{numeric scalar: A fixed number of days from the event start date}
#'  \item{"event_end_date"}: The event end date. If the event end date is not populated then the event start date will be used
#' }
#' @param subsetCohort,subset_cohort  A cohort table containing the individuals for which to
#' generate cohorts for. Only individuals in the cohort table will appear in
#' the created generated cohort set.
#' @param subsetCohortId,subset_cohort_id A set of cohort IDs from the cohort table for which
#' to include. If none are provided, all cohorts in the cohort table will
#' be included.
#' @param overwrite Should the cohort table be overwritten if it already exists? TRUE (default) or FALSE.
#'
#' @return A cdm reference object with the new generated cohort set table added
#' @export
generateConceptCohortSet <- function(cdm,
                                     conceptSet = NULL,
                                     name,
                                     limit = "first",
                                     requiredObservation = c(0,0),
                                     end = "observation_period_end_date",
                                     subsetCohort = NULL,
                                     subsetCohortId = NULL,
                                     overwrite = TRUE,
                                     endArgs = list(
                                       conceptSet = conceptSet,
                                       persistenceWindow = 30L,
                                       surveillanceWindow = 0L,
                                       daysSupplyOverride = NULL,
                                       index = c("startDate"),
                                       offsetDays = 7
                                     ),
                                     containsSourceConceptIds = FALSE
                                     ) {
  # check cdm ----
  checkmate::assertClass(cdm, "cdm_reference")
  con <- cdmCon(cdm)
  checkmate::assertTRUE(DBI::dbIsValid(cdmCon(cdm)))
  checkmate::assert_character(name, len = 1, min.chars = 1, any.missing = FALSE, pattern = "[a-zA-Z0-9_]+")

  .assertTables(cdm, "observation_period", empty.ok = FALSE)

  # check name ----
  checkmate::assertLogical(overwrite, len = 1, any.missing = FALSE)
  checkmate::assertCharacter(name, len = 1, any.missing = FALSE, min.chars = 1, pattern = "[a-z1-9_]+")
  existingTables <- listTables(con, cdmWriteSchema(cdm))

  if (name %in% existingTables && !overwrite) {
    rlang::abort(glue::glue("{name} already exists in the CDM write_schema and overwrite is FALSE!"))
  }

  # check limit ----
  checkmate::assertChoice(limit, c("first", "all", 'last'))

  # check requiredObservation ----
  checkmate::assertIntegerish(requiredObservation, lower = 0, any.missing = FALSE, len = 2)

  # check end ----
  if (is.numeric(end)) {
    checkmate::assertIntegerish(end, lower = 0L, len = 1)
  } else if (is.character(end)) {
    checkmate::assertCharacter(end, len = 1)
    checkmate::assertChoice(end, choices = c(
      "observation_period_end_date",
      "event_end_date",
      "drug_exit")
      )
  } else {
    rlang::abort('`end` must be a natural number of days from start, "observation_period_end_date", or "event_end_date"')
  }

  # check ConceptSet ----
  checkmate::assertList(conceptSet, min.len = 1, any.missing = FALSE,
                        types = c("numeric", "ConceptSet", "conceptSetExpression"),
                        names = "named")
  checkmate::assertList(conceptSet, min.len = 1, names = "named")
  .assertTables(cdm, "concept")

  if (methods::is(conceptSet, "conceptSetExpression")) {
    # omopgenerics conceptSetExpression
    conceptSets <- purrr::map2(conceptSet, names(conceptSet), .expr2CaprCs)

  } else if (methods::is(conceptSet[[1]], "ConceptSet")) {
    #Capr concept sets
    conceptSets <- conceptSet
  } else {
    # conceptSet should be a named list of int vectors
    # remove any empty concept sets
    conceptSet <- conceptSet[lengths(conceptSet) > 0]
    # conceptSet must be a named list of integer-ish vectors
    purrr::walk(conceptSet, ~checkmate::assert_integerish(., lower = 0, min.len = 1, any.missing = FALSE))
    conceptSets <- purrr::map2(conceptSet, names(conceptSet), .codelist2CaprCs)
  }

  # collect concept set details
  conceptSets <- purrr::map(conceptSets, ~ .getCaprCsDetails(.x, cdm))

  # collect Capr cohort objects
  cohorts <- purrr::map(conceptSets, ~ createConceptBasedCaprCohort(.x))

  if (containsSourceConceptIds) cohorts <- purrr::map(cohorts, .addSourceConceptEntry)


  cohortsToCreate <- dplyr::tibble(
    cohort_definition_id = seq_along(cohorts),
    cohort_name = names(conceptSets)
    ) %>%
    dplyr::mutate(cohort = cohorts) %>%
    dplyr::mutate(json = purrr::map(.data$cohort, RJSONIO::toJSON)) %>%
    dplyr::mutate(cohort_name = stringr::str_replace_all(tolower(.data$cohort_name), "\\s", "_")) %>%
    dplyr::mutate(cohort_name = stringr::str_remove_all(.data$cohort_name, "[^a-z0-9_]")) %>%
    dplyr::mutate(cohort_definition_id = dplyr::if_else(stringr::str_detect(.data$cohort_name, "^[0-9]+$"), suppressWarnings(as.integer(.data$cohort_name)), .data$cohort_definition_id)) %>%
    dplyr::mutate(cohort_name = dplyr::if_else(stringr::str_detect(.data$cohort_name, "^[0-9]+$"), paste0("cohort_", .data$cohort_name), .data$cohort_name))
  # snakecase name can be used for column names or filenames
  cohortsToCreate <- cohortsToCreate %>%
    dplyr::mutate(cohort_name_snakecase = snakecase::to_snake_case(.data$cohort_name)) %>%
    dplyr::select("cohort_definition_id", "cohort_name", "cohort", "json", "cohort_name_snakecase")
  for (i in seq_len(nrow(cohortsToCreate))) {
    first_chr <- substr(cohortsToCreate$cohort_name[i], 1, 1)
    if (!grepl("[a-zA-Z]", first_chr)) {
      cli::cli_abort("Cohort names must start with a letter but {cohortsToCreate$cohort_name[i]} does not.
                     Rename the json file or use a CohortsToCreate.csv file to explicity set cohort names.")
    }
  }
  class(cohortsToCreate) <- c("CohortSet", class(cohortsToCreate))
  cdm <- generateCohortSet(
    cdm = cdm,
    cohortSet = cohortsToCreate,
    name = name,
    computeAttrition = TRUE,
    overwrite = overwrite)

  # check target cohort -----
  if (!is.null(subsetCohort)) {
    .assertTables(cdm, subsetCohort)
  }

  if (!is.null(subsetCohort) && !is.null(subsetCohortId)){
    if (!nrow(omopgenerics::settings(cdm[[subsetCohort]]) %>% dplyr::filter(.data$cohort_definition_id %in% .env$subsetCohortId)) > 0){
      cli::cli_abort("cohort_definition_id {subsetCohortId} not found in cohort set of {subsetCohort}")
   }}
    return(cdm)
}

#' `r lifecycle::badge("deprecated")`
#' @rdname generateConceptCohortSet
#' @export
generate_concept_cohort_set <- function(cdm,
                                        concept_set = NULL,
                                        name = "cohort",
                                        limit = "first",
                                        required_observation = c(0,0),
                                        end = "observation_period_end_date",
                                        subset_cohort = NULL,
                                        subset_cohort_id = NULL,
                                        overwrite = TRUE) {
  lifecycle::deprecate_soft("1.7.0", "generate_concept_cohort_set()", "generateConceptCohortSet()")
  generateConceptCohortSet(cdm = cdm,
                           conceptSet = concept_set,
                           name = name,
                           limit = limit,
                           requiredObservation = required_observation,
                           end = end,
                           subsetCohort = subset_cohort,
                           subsetCohortId = subset_cohort_id,
                           overwrite = overwrite)
}




.csExpr2CsCapr <- function(concept_id, excluded, descendants, mapped = FALSE) {
  if (excluded & descendants) {
    expr <- rlang::expr(Capr::cs(
      Capr::exclude(Capr::descendants(!!concept_id)),
      name = !!glue::glue("name_{concept_id}")
    ))
  } else if (excluded) {
    expr <- rlang::expr(Capr::cs(
      Capr::exclude(!!concept_id),
      name = !!glue::glue("name_{concept_id}")
    ))
  } else if (descendants) {
    expr <- rlang::expr(Capr::cs(
      Capr::descendants(!!concept_id),
      name = !!glue::glue("name_{concept_id}")
    ))
  } else {
    expr <- rlang::expr(Capr::cs(
      !!concept_id,
      name = !!glue::glue("name_{concept_id}")
    ))
  }
  return(expr)
}
.expr2CaprCs <- function(x, .name) {
  tmp_file <- fs::file_temp(ext = "csv")
  on.exit(unlink(tmp_file, recursive = TRUE))
  readr::write_csv(
    purrr::pmap(x, .csExpr2CsCapr) |>
      purrr::map_dfr(~ .buildCsTable(.x)) |>
      dplyr::distinct(),
    tmp_file
  )
  return(Capr::readConceptSet(tmp_file, name = .name))
}
.codelist2CaprCs <- function(x, .name) {
  cs <- Capr::cs(x, name = .name)
}
.getCaprCsDetails <- function(x, cdm) {
  ids <- purrr::map_int(x@Expression, ~ .@Concept@concept_id)
  df <- cdm[["concept"]] |>
    dplyr::filter(.data$concept_id %in% ids) %>%
    dplyr::collect() |>
    tibble::tibble() %>%
    dplyr::mutate(
      invalid_reason = ifelse(is.na(.data$invalid_reason),
                              "V", .data$invalid_reason
      )
    ) %>%
    dplyr::mutate(
      standard_concept_caption = dplyr::case_when(
        standard_concept == "S" ~ "Standard",
        standard_concept == "N" ~ "Non-Standard",
        standard_concept == "C" ~ "Classification", TRUE ~ ""
      )
    ) %>%
    dplyr::mutate(invalid_reason_caption = dplyr::case_when(
      invalid_reason == "V" ~ "Valid",
      invalid_reason == "I" ~ "Invalid",
      TRUE ~ ""
    ))
  checkSlotNames <- methods::slotNames("Concept")[-1]
  for (i in seq_along(x@Expression)) {
    id <- x@Expression[[i]]@Concept@concept_id
    for (n in checkSlotNames) {
      dtl <- dplyr::filter(df, .data$concept_id == id) %>%
        dplyr::pull(!!n)
      if (length(dtl > 0)) {
        methods::slot(x@Expression[[i]]@Concept, n) <- dtl
      }
    }
  }
  return(x)
}
.buildCsTable <- function(expr) {
  x <- eval(expr)
  tibble::tibble(concept_set_name = x@Name, concept_id = purrr::map_int(
    x@Expression,
    ~ .@Concept@concept_id
  ), concept_name = purrr::map_chr(
    x@Expression,
    ~ .@Concept@concept_name
  ), domain_id = purrr::map_chr(
    x@Expression,
    ~ .@Concept@domain_id
  ), vocabulary_id = purrr::map_chr(
    x@Expression,
    ~ .@Concept@vocabulary_id
  ), concept_class_id = purrr::map_chr(
    x@Expression,
    ~ .@Concept@concept_class_id
  ), standard_concept = purrr::map_chr(
    x@Expression,
    ~ .@Concept@standard_concept
  ), standard_concept_caption = purrr::map_chr(
    x@Expression,
    ~ .@Concept@standard_concept_caption
  ), concept_code = purrr::map_chr(
    x@Expression,
    ~ .@Concept@concept_code
  ), invalid_reason = purrr::map_chr(
    x@Expression,
    ~ .@Concept@invalid_reason
  ), invalid_reason_caption = purrr::map_chr(
    x@Expression,
    ~ .@Concept@invalid_reason_caption
  ), includeDescendants = purrr::map_lgl(
    x@Expression,
    "includeDescendants"
  ), isExcluded = purrr::map_lgl(
    x@Expression,
    "isExcluded"
  ), includeMapped = purrr::map_lgl(
    x@Expression,
    "includeMapped"
  ))
}
.addSourceConceptEntry <- function(cohort) {

  domainBlock <- cohort$PrimaryCriteria$CriteriaList[[1]] |>
    names() |>
    split_on_second_uppercase()

  occurrence <- cohort$PrimaryCriteria$CriteriaList[[1]] |>
    names()

  sourceCriteria <- paste0(domainBlock[[1]], 'SourceConcept')

  cohort$PrimaryCriteria$CriteriaList[[2]] <- cohort$PrimaryCriteria$CriteriaList[[1]]

  names(cohort$PrimaryCriteria$CriteriaList[[2]][[occurrence]]) <- sourceCriteria

  return(cohort)

}

