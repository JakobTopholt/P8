PYTHON ?= python
CONFIG ?= configs/mvp.example.yaml
STUDY_REGION_FILE ?=
ZONES_FILE ?=
CORRIDOR_FILE ?=
CORRIDOR_BUFFER_METERS ?=

.PHONY: bootstrap load-context build-trajectories compute-labels create-dev-subset sprint1 run-baselines summarize-baselines export-visual-inspection export-qgis-inspection status test

bootstrap:
	$(PYTHON) -m src.cli --config $(CONFIG) bootstrap

load-context:
	$(PYTHON) -m src.cli --config $(CONFIG) load-context --study-region-file "$(STUDY_REGION_FILE)" --zones-file "$(ZONES_FILE)" --corridor-file "$(CORRIDOR_FILE)" $(if $(CORRIDOR_BUFFER_METERS),--corridor-buffer-meters $(CORRIDOR_BUFFER_METERS),)

build-trajectories:
	$(PYTHON) -m src.cli --config $(CONFIG) build-trajectories

compute-labels:
	$(PYTHON) -m src.cli --config $(CONFIG) compute-labels

create-dev-subset:
	$(PYTHON) -m src.cli --config $(CONFIG) create-dev-subset

sprint1:
	$(PYTHON) -m src.cli --config $(CONFIG) sprint1

run-baselines:
	$(PYTHON) -m src.cli --config $(CONFIG) run-baselines --overwrite

summarize-baselines:
	$(PYTHON) -m src.cli --config $(CONFIG) summarize-baselines

export-visual-inspection:
	$(PYTHON) -m src.cli --config $(CONFIG) export-visual-inspection

export-qgis-inspection:
	$(PYTHON) -m src.cli --config $(CONFIG) export-qgis-inspection

status:
	$(PYTHON) -m src.cli --config $(CONFIG) status

test:
	$(PYTHON) -m pytest tests -q
