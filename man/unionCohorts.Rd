% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/zzz-deprecated.R
\name{unionCohorts}
\alias{unionCohorts}
\alias{union_cohorts}
\title{Union all cohorts in a single cohort table}
\usage{
unionCohorts(x, cohortDefinitionId = 1L)

union_cohorts(x, cohort_definition_id = 1L)
}
\arguments{
\item{x}{A tbl reference to a cohort table}

\item{cohort_definition_id, cohortDefinitionId}{A number to use for the new cohort_definition_id}
}
\value{
A lazy query that when executed will resolve to a new cohort table with
one cohort_definition_id resulting from the union of all cohorts in the original
cohort table
}
\description{
Union all cohorts in a single cohort table

\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#deprecated}{\figure{lifecycle-deprecated.svg}{options: alt='[Deprecated]'}}}{\strong{[Deprecated]}}
}
