% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/generateCohortSet.R
\name{record_cohort_attrition}
\alias{record_cohort_attrition}
\title{Add attrition reason to a cohort_table object}
\usage{
record_cohort_attrition(cohort, reason, cohortId = NULL)
}
\arguments{
\item{cohort}{A generated cohort set}

\item{reason}{The reason for attrition as a character string}

\item{cohortId}{Cohort definition id of the cohort you want to update the
attrition}
}
\value{
The cohort object with the attributes created or updated.

\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#deprecated}{\figure{lifecycle-deprecated.svg}{options: alt='[Deprecated]'}}}{\strong{[Deprecated]}}
}
\description{
Update the cohort attrition table with new counts and a reason for attrition.
}
