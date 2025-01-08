capr_ref <- function(domain_id) {
  entry <- dplyr::tribble(
    ~domain_id,  ~capr_spec,
    "condition",  "conditionOccurrence",
    "drug",  "drugExposure",
    "procedure",  "procedure",
    "observation",  "observation",
    "measurement",   "measurement",
    "visit",  "visit",
    "device",  "deviceExposure") |>
    dplyr::filter(.data$domain_id %in% .env$domain_id) |>
    dplyr::pull(.data$capr_spec)
}

createConceptBasedCaprCohort <- function(
    conceptSet,
    limit = "first",
    requiredObservation = c(0,0),
    end = "observation_period_end_date",
    endArgs = list(
      conceptSet = conceptSet,
      persistenceWindow = 30L,
      surveillanceWindow = 0L,
      daysSupplyOverride = NULL,
      index = c("startDate"),
      offsetDays = 7
    )
) {

  # checkmate::assert_class(conceptSet, "ConceptSet")
  domains <- purrr::pluck(
    conceptSet, "Expression") |>
    purrr::map_chr(~ .x@Concept@domain_id) |>
    unique() |> tolower()

  .exit <- checkmate::matchArg(
    end,
    c(
      "observation_period_end_date",
      "drug_exit",
      "event_end_date"
    )
  ) %>% gsub('_period_end_date', '_exit',.) %>%
    gsub('event_end_date', 'fixed_exit',.) |>
    SqlRender::snakeCaseToCamelCase()

  fnsCalls <- purrr::map_chr(domains, ~glue::glue('Capr::{capr_ref(.x)}')) |>
    purrr::map( ~ .prep_call(.x, list(conceptSet = conceptSet )))
  .evals <- purrr::map_chr(
    seq_along(fnsCalls),
    ~ glue::glue('eval(fnsCalls[[{.x}]])')) |>
    paste(collapse = ',')
  args <- list(
    observationWindow = .prep_call('Capr::continuousObservation ', list(
      priorDays = requiredObservation[[1]],
      postDays  = requiredObservation[[2]]
    )),
    primaryCriteriaLimit = checkmate::matchArg(limit, c("first", "all", "last")) |>
      snakecase::to_any_case(case = "title") ,
    endStrategy = .prep_call(paste0('Capr::', .exit), endArgs)
  )
  cohortAttrs <- list(
    entry = eval(str2lang(glue::glue('Capr::entry({.evals})'))),
    exit = .prep_call('Capr::exit', args)
  )
  return(Capr::toCirce(eval(.prep_call('Capr::cohort', cohortAttrs))))
}

.prep_call <- function(fn_name, args) {
  rlang::call2(
    eval(str2lang(fn_name)), !!!purrr::keep_at(
      args, names(args) %in%
        rlang::fn_fmls_names(
          eval(str2lang(fn_name))
        )
    )
  )
}


split_on_second_uppercase <- function(string) {
  # Find the positions of all uppercase letters
  positions <- stringr::str_locate_all(string, "[A-Z]")[[1]][,1]

  # Check if there are at least two uppercase letters
  if (length(positions) < 2) {
    return(string)
  }

  # Split the string at the second uppercase letter
  second_uppercase_pos <- positions[2]
  part1 <- substr(string, 1, second_uppercase_pos - 1)
  part2 <- substr(string, second_uppercase_pos, nchar(string))

  return(c(part1))
}
