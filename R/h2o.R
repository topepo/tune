extract_h2o_algorithm <- function(workflow, ...) {
  model_spec <- hardhat::extract_spec_parsnip(workflow)
  model_class <- class(model_spec)[1]
  all_algos <- c("boost_tree", "rand_forest", "linear_reg", "logistic_reg",
                 "multinom_reg", "mlp", "naive_Bayes")
  algo <- switch(model_class,
         boost_tree = "gbm",
         rand_forest = "randomForest",
         linear_reg  = "glm",
         logistic_reg = "glm",
         multinom_reg = "glm",
         mlp = "deeplearning",
         naive_Bayes = "naive_bayes",
         rlang::abort(
           glue::glue("Model `{model_class}` is not supported by the h2o engine, use one of { toString(all_algos) }")
         )
    )
  algo
}


as_h2o <- function(df, destination_frame_prefix) {
  id <- paste(destination_frame_prefix, runif(1), sep = "_")
  list(
    data = as.h2o(df, destination_frame = id),
    id = id
  )
}

is_h2o <- function(workflow, ...) {
  model_spec <- hardhat::extract_spec_parsnip(object)
  identical(model_spec$engine, "h2o")
}


tune_grid_loop_iter_h2o <- function(split,
                                    grid_info,
                                    workflow,
                                    metrics,
                                    control,
                                    seed) {
  load_pkgs(workflow)
  load_namespace(control$pkgs)

  training_frame <- rsample::analysis(split)
  val_frame <- rsample::assessment(split)
  mode <- hardhat::extract_spec_parsnip(wf)$mode
  workflow_original <- wf

  pset <- hardhat::extract_parameter_set_dials(workflow_original)
  param_names <- dplyr::pull(pset, "id")
  model_params <- dplyr::filter(pset, source == "model_spec")
  preprocessor_params <- dplyr::filter(pset, source == "recipe")
  model_param_names <- dplyr::pull(model_params, "id")
  preprocessor_param_names <- dplyr::pull(preprocessor_params, "id")


  outcome_name <- outcome_names(workflow)
  event_level <- control$event_level
  orig_rows <- as.integer(split, data = "assessment")


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

    iter_grid_info_models <- iter_grid_info[["data"]][[1L]] %>%
      dplyr::select(dplyr::all_of(model_param_names))

    iter_grid <- dplyr::bind_cols(
      iter_grid_preprocessor,
      iter_grid_info_models
    )

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

    h2o_training_frame <- as_h2o(training_frame_processed, "training_frame")
    h2o_val_frame <- as_h2o(val_frame_processed, "val_frame")

    h2o_algo <- extract_h2o_algorithm(workflow)
    h2o_res <- h2o::h2o.grid(
      h2o_algo,
      x = predictors,
      y = outcome,
      training_frame = h2o_training_frame$data,
      hyper_params = h2o_hyper_params
    )

    h2o_model_ids <- as.character(h2o_res@model_ids)
    h2o_models <- purrr::map(h2o_model_ids, h2o.getModel)

    val_truth <- val_frame_processed[outcome_name]
    h2o_preds <- purrr::map(h2o_models, pull_h2o_predictions,
                            h2o_val_frame$data, val_truth, orig_rows, mode) %>%
      purrr::imap(~ bind_cols(.x, iter_grid[.y, ]))

    # yardstick metrics
    h2o_metrics <- purrr::map(h2o_preds, \(h2o_pred) {
      estimate_metrics_safely <- safely(estimate_metrics)
      metrics <- estimate_metrics_safely(h2o_pred, metrics, param_names,
                              outcome_name, event_level)
      if (is.null(metrics$error)) {
        metrics$result
      } else {
        metrics$error
      }
    })

    grid_out <- iter_grid_info %>%
      tidyr::unnest(cols = data) %>%
      dplyr::select(
        dplyr::all_of(preprocessor_param_names),
        dplyr::all_of(model_param_names),
        .iter_config
      ) %>%
      tidyr::unnest(.iter_config) %>%
      dplyr::mutate(.metrics = h2o_metrics)

    if (control$save_pred) {
      grid_out <- grid_out %>% dplyr::mutate(.predictions = h2o_preds)
    }

    out[[iter_preprocessor]] <- grid_out

    # remove objects from h2o server
    h2o::h2o.rm(c(h2o_model_ids, h2o_training_frame$id, h2o_val_frame$id))
  }

  out <- vctrs::vec_rbind(!!!out) %>%
    dplyr::bind_cols(labels(split))

  out
}


pull_h2o_predictions <- function(h2o_model, val_frame, val_truth, orig_rows, mode) {
  h2o_preds <- h2o::h2o.predict(h2o_model, val_frame) %>%
    tibble::as_tibble()

  if (mode == "classification") {
    h2o_preds <- format_classprobs(h2o_preds %>% dplyr::select(-predict)) %>%
      dplyr::bind_cols(format_class(h2o_preds %>% purrr::pluck("predict")))
  } else {
    h2o_preds <- format_num(h2o_preds %>% purrr::pluck("predict"))
  }

  h2o_preds %>%
    dplyr::mutate(.row = orig_rows) %>%
    dplyr::bind_cols(val_truth)
}



pull_h2o_metrics <- function(h2o_model, val_frame) {
  metrics <- slot(h2o::h2o.performance(h2o_model, val_frame), "metrics")
  metrics
}
