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

CLI = $(PYTHON) -m src.cli --config $(CONFIG)

.PHONY: help bootstrap load-context build-trajectories compute-labels create-dev-subset prepare-data benchmark summarize-baselines inspect-html inspect-qgis tune-postgres doctor status test sprint1 run-baselines export-visual-inspection export-qgis-inspection subset preflight

help:
	@printf "AIS-Contextual-QDS Make Targets\n\n"
	@printf "Current default config: %s\n\n" "$(CONFIG)"
	@printf "Setup and data prep:\n"
	@printf "  make bootstrap\n"
	@printf "  make load-context STUDY_REGION_FILE=... ZONES_FILE=... CORRIDOR_FILE=... [CORRIDOR_BUFFER_METERS=700]\n"
	@printf "  make build-trajectories\n"
	@printf "  make compute-labels [MODE=optimized|segment_exact]\n"
	@printf "  make create-dev-subset\n"
	@printf "  make prepare-data [MODE=optimized|segment_exact]\n\n"
	@printf "Evaluation and inspection:\n"
	@printf "  make benchmark [EVALUATION_MODE=optimized|segment_exact] [TRUTH_LABEL_MODE=optimized|segment_exact] [RUN_TAG=...]\n"
	@printf "  make summarize-baselines [RUN_TAG=...]\n"
	@printf "  make inspect-html [EVALUATION_MODE=optimized|segment_exact] [TRUTH_LABEL_MODE=optimized|segment_exact] [RUN_TAG=...]\n"
	@printf "  make inspect-qgis [EVALUATION_MODE=optimized|segment_exact] [TRUTH_LABEL_MODE=optimized|segment_exact] [RUN_TAG=...]\n"
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

prepare-data:
	$(CLI) prepare-data $(if $(MODE),--label-mode $(MODE),)

benchmark:
	$(CLI) benchmark --overwrite $(if $(EVALUATION_MODE),--evaluation-mode $(EVALUATION_MODE),) $(if $(TRUTH_LABEL_MODE),--truth-label-mode $(TRUTH_LABEL_MODE),) $(if $(RUN_TAG),--run-tag $(RUN_TAG),)

summarize-baselines:
	$(CLI) summarize-baselines $(if $(RUN_TAG),--run-tag $(RUN_TAG),)

inspect-html:
	$(CLI) inspect-html $(if $(EVALUATION_MODE),--evaluation-mode $(EVALUATION_MODE),) $(if $(TRUTH_LABEL_MODE),--truth-label-mode $(TRUTH_LABEL_MODE),) $(if $(RUN_TAG),--run-tag $(RUN_TAG),)

inspect-qgis:
	$(CLI) inspect-qgis $(if $(EVALUATION_MODE),--evaluation-mode $(EVALUATION_MODE),) $(if $(TRUTH_LABEL_MODE),--truth-label-mode $(TRUTH_LABEL_MODE),) $(if $(RUN_TAG),--run-tag $(RUN_TAG),)

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
