% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/cohortTransformations.R
\name{cohortErafy}
\alias{cohortErafy}
\alias{cohort_erafy}
\title{Collapse cohort records within a certain number of days}
\usage{
cohortErafy(x, gap)

cohort_erafy(x, gap)
}
\arguments{
\item{x}{A generated cohort set}

\item{gap}{When two cohort records are 'gap' days apart or less the periods will be
collapsed into a single record}
}
\value{
A lazy query on a generated cohort set
}
\description{
Collapse cohort records within a certain number of days

\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#deprecated}{\figure{lifecycle-deprecated.svg}{options: alt='[Deprecated]'}}}{\strong{[Deprecated]}}
}
