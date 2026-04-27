PYTHON ?= $(shell if [ -x ../.venv/bin/python ]; then printf '%s' ../.venv/bin/python; elif [ -x .venv/bin/python ]; then printf '%s' .venv/bin/python; else printf '%s' python; fi)
CONFIG ?= configs/iteration1_10days.example.yaml
STUDY_REGION_FILE ?=
ZONES_FILE ?=
CORRIDOR_FILE ?=
CORRIDOR_BUFFER_METERS ?=
MODE ?=
EVALUATION_MODE ?=
TRUTH_LABEL_MODE ?=
RUN_TAG ?=
METHODS ?=
BUDGET ?=
BUDGETS ?=
METHOD ?=
SUBSET_NAME ?=
DEV_SIZE ?=
EVAL_SIZE ?=
BASE_MODE ?= optimized
CANDIDATE_MODE ?= segment_exact
SPLIT ?=
POSITIVE_FRACTION ?=
MIN_ZONE_POSITIVES ?=
MIN_CORRIDOR_POSITIVES ?=

CLI = $(PYTHON) -m src.cli --config $(CONFIG)

.PHONY: help bootstrap load-context build-trajectories compute-labels create-dev-subset create-hardcase-subset label-balance compare-label-modes prepare-data benchmark summarize-baselines inspect-html inspect-qgis tune-postgres doctor status test sprint1 run-baselines export-visual-inspection export-qgis-inspection subset preflight audit-label-modes

help:
	@printf "AIS-Contextual-QDS Make Targets\n\n"
	@printf "Current default config: %s\n\n" "$(CONFIG)"
	@printf "Setup and data prep:\n"
	@printf "  make bootstrap\n"
	@printf "  make load-context STUDY_REGION_FILE=... ZONES_FILE=... CORRIDOR_FILE=... [CORRIDOR_BUFFER_METERS=700]\n"
	@printf "  make build-trajectories\n"
	@printf "  make compute-labels [MODE=optimized|segment_exact]\n"
	@printf "  make create-dev-subset\n"
	@printf "  make create-hardcase-subset [SUBSET_NAME=...] [MODE=optimized|segment_exact] [DEV_SIZE=...] [EVAL_SIZE=...]\n"
	@printf "  make prepare-data [MODE=optimized|segment_exact]\n\n"
	@printf "Evaluation and inspection:\n"
	@printf "  make label-balance [MODE=optimized|segment_exact] [SUBSET_NAME=...]\n"
	@printf "  make compare-label-modes [BASE_MODE=optimized] [CANDIDATE_MODE=segment_exact] [SUBSET_NAME=...] [SPLIT=dev|eval]\n"
	@printf "  make benchmark [METHODS=uniform,dp] [BUDGETS=0.1,0.2] [SUBSET_NAME=...] [EVALUATION_MODE=optimized|segment_exact] [TRUTH_LABEL_MODE=optimized|segment_exact] [RUN_TAG=...]\n"
	@printf "  make summarize-baselines [RUN_TAG=...]\n"
	@printf "  make inspect-html [RUN_TAG=...] [METHOD=...] [BUDGET=...] [SUBSET_NAME=...] [SPLIT=dev|eval]\n"
	@printf "  make inspect-qgis [RUN_TAG=...] [METHOD=...] [BUDGET=...] [SUBSET_NAME=...] [SPLIT=dev|eval]\n"
	@printf "  make status\n"
	@printf "  make doctor\n"
	@printf "  make tune-postgres\n"
	@printf "  make test\n"

bootstrap:
	$(CLI) bootstrap

load-context:
	$(CLI) load-context --study-region-file "$(STUDY_REGION_FILE)" --zones-file "$(ZONES_FILE)" --corridor-file "$(CORRIDOR_FILE)" $(if $(CORRIDOR_BUFFER_METERS),--corridor-buffer-meters $(CORRIDOR_BUFFER_METERS),)

build-trajectories:
	$(CLI) build-trajectories

compute-labels:
	$(CLI) compute-labels $(if $(MODE),--mode $(MODE),)

create-dev-subset:
	$(CLI) create-dev-subset

create-hardcase-subset:
	$(CLI) create-hardcase-subset $(if $(SUBSET_NAME),--subset-name $(SUBSET_NAME),) $(if $(MODE),--mode $(MODE),) $(if $(DEV_SIZE),--dev-size $(DEV_SIZE),) $(if $(EVAL_SIZE),--eval-size $(EVAL_SIZE),) $(if $(POSITIVE_FRACTION),--positive-fraction $(POSITIVE_FRACTION),) $(if $(MIN_ZONE_POSITIVES),--min-zone-positives-per-split $(MIN_ZONE_POSITIVES),) $(if $(MIN_CORRIDOR_POSITIVES),--min-corridor-positives-per-split $(MIN_CORRIDOR_POSITIVES),)

label-balance:
	$(CLI) label-balance $(if $(MODE),--mode $(MODE),) $(if $(SUBSET_NAME),--subset-name $(SUBSET_NAME),) $(if $(MIN_ZONE_POSITIVES),--min-zone-positives $(MIN_ZONE_POSITIVES),) $(if $(MIN_CORRIDOR_POSITIVES),--min-corridor-positives $(MIN_CORRIDOR_POSITIVES),)

compare-label-modes:
	$(CLI) compare-label-modes --base-mode $(BASE_MODE) --candidate-mode $(CANDIDATE_MODE) $(if $(SUBSET_NAME),--subset-name $(SUBSET_NAME),) $(if $(SPLIT),--split $(SPLIT),)

prepare-data:
	$(CLI) prepare-data $(if $(MODE),--label-mode $(MODE),)

benchmark:
	$(CLI) benchmark --overwrite $(if $(METHODS),--methods $(METHODS),) $(if $(BUDGETS),--budgets $(BUDGETS),) $(if $(SUBSET_NAME),--subset-name $(SUBSET_NAME),) $(if $(SPLIT),--split $(SPLIT),) $(if $(EVALUATION_MODE),--evaluation-mode $(EVALUATION_MODE),) $(if $(TRUTH_LABEL_MODE),--truth-label-mode $(TRUTH_LABEL_MODE),) $(if $(RUN_TAG),--run-tag $(RUN_TAG),)

summarize-baselines:
	$(CLI) summarize-baselines $(if $(RUN_TAG),--run-tag $(RUN_TAG),)

inspect-html:
	$(CLI) inspect-html $(if $(EVALUATION_MODE),--evaluation-mode $(EVALUATION_MODE),) $(if $(TRUTH_LABEL_MODE),--truth-label-mode $(TRUTH_LABEL_MODE),) $(if $(RUN_TAG),--run-tag $(RUN_TAG),) $(if $(METHOD),--method $(METHOD),) $(if $(BUDGET),--budget $(BUDGET),) $(if $(SUBSET_NAME),--subset-name $(SUBSET_NAME),) $(if $(SPLIT),--split $(SPLIT),)

inspect-qgis:
	$(CLI) inspect-qgis $(if $(EVALUATION_MODE),--evaluation-mode $(EVALUATION_MODE),) $(if $(TRUTH_LABEL_MODE),--truth-label-mode $(TRUTH_LABEL_MODE),) $(if $(RUN_TAG),--run-tag $(RUN_TAG),) $(if $(METHOD),--method $(METHOD),) $(if $(BUDGET),--budget $(BUDGET),) $(if $(SUBSET_NAME),--subset-name $(SUBSET_NAME),) $(if $(SPLIT),--split $(SPLIT),)

tune-postgres:
	$(CLI) tune-postgres --profile laptop_safe

doctor:
	$(CLI) doctor

status:
	$(CLI) status

test:
	$(PYTHON) -m pytest tests -q

# Backward-compatible aliases
sprint1: prepare-data
run-baselines: benchmark
export-visual-inspection: inspect-html
export-qgis-inspection: inspect-qgis
subset: create-dev-subset
preflight: doctor
audit-label-modes: compare-label-modes
