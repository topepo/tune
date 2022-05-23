format_num <- function(x) {
  if (inherits(x, "tbl_spark"))
    return(x)

  if (isTRUE(ncol(x) > 1) | is.data.frame(x)) {
    x <- tibble::as_tibble(x, .name_repair = "minimal")
    if (!any(grepl("^\\.pred", names(x)))) {
      names(x) <- paste0(".pred_", names(x))
    }
  } else {
    x <- tibble::tibble(.pred = unname(x))
  }

  x
}

format_class <- function(x) {
  if (inherits(x, "tbl_spark"))
    return(x)

  tibble::tibble(.pred_class = unname(x))
}

format_classprobs <- function(x) {
  if (!any(grepl("^\\.pred_", names(x)))) {
    names(x) <- paste0(".pred_", names(x))
  }
  x <- tibble::as_tibble(x)
  x <- purrr::map_dfr(x, rlang::set_names, NULL)
  x
}

format_linear_pred <- function(x) {
  if (inherits(x, "tbl_spark"))
    return(x)

  if (isTRUE(ncol(x) > 1) | is.data.frame(x)) {
    x <- tibble::as_tibble(x, .name_repair = "minimal")
    names(x) <- ".pred_linear_pred"
  } else {
    x <- tibble::tibble(.pred_linear_pred = unname(x))
  }

  x
}
