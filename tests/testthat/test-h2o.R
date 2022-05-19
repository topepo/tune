library(h2oparsnip)
library(modeldata)
library(tidymodels)
library(h2o)
h2o.init()

data(two_class_dat)
doParallel::registerDoParallel()
# model and workflow spec
folds <- vfold_cv(two_class_dat, nfolds = 10)
glm_spec <- logistic_reg(penalty = tune("lambda")) |>
    set_engine("h2o")

rec <- recipe(Class ~ A + B, two_class_dat) %>%
    step_ns(A, deg_free = tune("spline df"))

wf <- workflow() %>%
    add_model(glm_spec) %>%
    add_recipe(rec)

# build grid_info and looping iterators
pset <- extract_parameter_set_dials(wf)
model_param_names <- dplyr::filter(pset, source == "model_spec")$id
grid <- dials::grid_regular(pset, levels = 5)

grid <- check_grid(grid = grid, workflow = wf, pset = pset)
grid_info <- compute_grid_info(wf, grid)
control <- control_grid()
packages <- c(control$pkgs, required_pkgs(wf))
n_resamples <- nrow(folds)
iterations <- seq_len(n_resamples)
n_grid_info <- nrow(grid_info)
rows <- seq_len(n_grid_info)
splits <- folds$splits

# model and preprocessor params

cols <- rlang::expr(
  c(
    .iter_model,
    .iter_config,
    .msg_model,
    dplyr::all_of(model_param_names),
    .submodels
  )
)
out_notes <-
  tibble::tibble(location = character(0), type = character(0), note = character(0))
grid_info_nest <- tidyr::nest(grid_info, data = !!cols)

# looping through resamples
seeds <- generate_seeds(rng = TRUE, n_resamples)

results <- foreach::foreach(
    split = splits,
    seed = seeds,
    .packages = packages,
    .errorhandling = "pass"
) %dopar% {
    tune_grid_loop_iter_h2o(
      split = split,
      grid_info = grid_info_nest,
      workflow = wf,
      seed = seed
    )
}

results[[1L]]
str(results[[1L]][[".metrics"]][[1L]], max.level = 2)

