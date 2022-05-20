#' Extract h2o algorithm from model and workflow
#' @export
extract_h2o_spec <- function(object, ...) {
  UseMethod("extract_h2o_spec")
}

extract_h2o_spec.default <- function(object, ...) {
  msg <- paste0(
    "The first argument to [extract_h2o_spec] should be either ",
    "a model or workflow."
  )
  rlang::abort(msg)
}

extract_h2o_spec.model_spec <- function(model_spec, ...) {
  if (inherits(model_spec, "boost_tree")) {
    model_name <- "boost_tree"
    algorithm <- "gbm"
  }
  if (inherits(model_spec, "rand_forest")) {
    model_name <- "rand_forest"
    algorithm <- "randomForest"
  }
  if (inherits(model_spec, "linear_reg")) {
    model_name <- "linear_reg"
    algorithm <- "glm"
  }
  if (inherits(model_spec, "logistic_reg")) {
    model_name <- "logistic_reg"
    algorithm <- "glm"
  }
  if (inherits(model_spec, "multinom_reg")) {
    model_name <- "multinom_reg"
    algorithm <- "glm"
  }
  if (inherits(model_spec, "mlp")) {
    model_name <- "mlp"
    algorithm <- "deeplearning"
  }
  if (inherits(model_spec, "naive_Bayes")) {
    model_name <- "naive_Bayes"
    algorithm <- "naiveBayes"
  }

  list(algorithm = algorithm, model_name = model_name)
}

extract_h2o_spec.workflow <- function(workflow, ...) {
  model_spec <- hardhat::extract_spec_parsnip(workflow)
  extract_h2o_spec(model_spec)
}

as_h2o <- function(df, destination_frame_prefix) {
  as.h2o(df, destination_frame = paste(destination_frame_prefix, runif(1), sep = "_"))
}

is_h2o <- function(object, ...) {
  UseMethod("is_h2o")
}

is_h2o.default <- function(object, ...) {
  msg <- paste0(
    "The first argument to [is_h2o] should be either ",
    "a model or workflow."
  )
  rlang::abort(msg)
}

is_h2o.model_spec <- function(object, ...) {
  identical(object$engine, "h2o")
}

is_h2o.workflow <- function(object, ...) {
  model_spec <- hardhat::extract_spec_parsnip(object)
  identical(model_spec$engine, "h2o")
}


tune_grid_loop_iter_h2o <- function(split,
                                    grid_info,
                                    workflow,
                                    seed,
                                    metrics = NULL,
                                    control = control_grid()) {
  load_pkgs(workflow)
  load_namespace(control$pkgs)

  training_frame <- rsample::analysis(split)
  val_frame <- rsample::assessment(split)
  workflow_original <- wf

  pset <- hardhat::extract_parameter_set_dials(workflow_original)
  model_params <- dplyr::filter(pset, source == "model_spec")
  preprocessor_params <- dplyr::filter(pset, source == "recipe")
  model_param_names <- dplyr::pull(model_params, "id")
  preprocessor_param_names <- dplyr::pull(preprocessor_params, "id")

  iter_preprocessors <- grid_info[[".iter_preprocessor"]]
  out <- vector("list", length(iter_preprocessors))
  # preprocessor loop
  for (iter_preprocessor in iter_preprocessors) {
    workflow <- workflow_original
    iter_grid_info <- dplyr::filter(
      .data = grid_info,
      .iter_preprocessor == iter_preprocessor
    )

    iter_grid_preprocessor <- dplyr::select(
      .data = iter_grid_info,
      dplyr::all_of(preprocessor_param_names)
    )

    iter_msg_preprocessor <- iter_grid_info[[".msg_preprocessor"]]

    # finalize and extract preprocessor
    workflow <- finalize_workflow_preprocessor(
      workflow = workflow,
      grid_preprocessor = iter_grid_preprocessor
    )
    workflow <- catch_and_log(
      .expr = .fit_pre(workflow, training_frame),
      control,
      split,
      iter_msg_preprocessor,
      notes = out_notes
    )

    preprocessor <- extract_recipe(workflow)

    # prep training and validation data
    training_frame_processed <- recipes::bake(preprocessor, new_data = training_frame)
    val_frame_processed <- recipes::bake(preprocessor, new_data = val_frame)
    iter_grid_info_models <- iter_grid_info[["data"]][[1L]]

    # extract outcome and predictor names (used by h2o.grid)
    outcome <- preprocessor$term_info %>%
      dplyr::filter(role == "outcome") %>%
      dplyr::pull("variable")

    predictors <- preprocessor$term_info %>%
      dplyr::filter(role == "predictor") %>%
      dplyr::pull("variable")

    # extract hyper params into list
    h2o_hyper_params <- purrr::map(
      model_param_names,
      ~ dplyr::pull(iter_grid_info_models, .)
      %>% unique()
    ) %>%
      purrr::set_names(model_param_names)

    training_frame_processed <- as_h2o(training_frame_processed, "training_frame")
    h2o_spec <- extract_h2o_spec.workflow(workflow)
    h2o_res <- h2o::h2o.grid(
      h2o_spec$algorithm,
      x = predictors,
      y = outcome,
      training_frame = training_frame_processed,
      hyper_params = h2o_hyper_params
    )

    h2o_model_ids <- h2o_res@model_ids
    h2o_models <- purrr::map(h2o_model_ids, h2o.getModel)
    h2o_preds <- purrr::map(h2o_models, pull_h2o_predictions, val_frame_processed, split)
    h2o_metrics <- purrr::map(h2o_models, pull_h2o_metrics, val_frame_processed)

    grid_out <- iter_grid_info %>%
      dplyr::unnest(cols = data) %>%
      dplyr::select(
        all_of(preprocessor_param_names),
        all_of(model_param_names),
        .iter_config
      ) %>%
      dplyr::unnest(.iter_config) %>%
      dplyr::mutate(
        .pred = h2o_preds,
        .metrics = h2o_metrics
      ) %>%
      dplyr::unnest(.pred)

    out[[iter_preprocessor]] <- grid_out
  }

  out <- vctrs::vec_rbind(!!!out) %>%
    bind_cols(labels(split))

  out
}

pull_h2o_predictions <- function(h2o_model, val_frame, split) {
  val_frame <- as_h2o(val_frame, "val_frame")

  orig_rows <- as.integer(split, data = "assessment")
  h2o_preds <- h2o::h2o.predict(h2o_model, val_frame)
  tibble::as_tibble(h2o_preds) %>% dplyr::mutate(.row = orig_rows)
}

pull_h2o_metrics <- function(h2o_model, val_frame) {
  val_frame <- as_h2o(val_frame, "val_frame")
  metrics <- slot(h2o::h2o.performance(h2o_model, val_frame), "metrics")
  metrics
}
