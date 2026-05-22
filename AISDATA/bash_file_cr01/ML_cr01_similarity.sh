#!/bin/bash

#SBATCH --job-name="AIS CR01 - similarity (train+QB)"
#SBATCH --mem=150G
#SBATCH --cpus-per-task=48
#SBATCH --gres=gpu:2
#SBATCH --time=12:00:00
#SBATCH --exclude=ailab-l4-04

PROJECT_DIR=/ceph/project/P8-1
HOME_DIR=/ceph/home/student.aau.dk/hq11zw/P8
TRAIN_DATE_TAG="7day_0202_to_0208"
EVAL_DATE_TAG="2026-02-10"
RUN_TAG="cr01_similarity"
SUBFOLDER="cr01"

TRAIN_CSV_PATH="${HOME_DIR}/AISDATA/preproessed_AIS_files/preprocessed_2026-02-02_to_02-08.7days.csv"
EVAL_CSV_PATH="${HOME_DIR}/AISDATA/preproessed_AIS_files/preprocessed_2026-02-10.csv"
if [ ! -f "$TRAIN_CSV_PATH" ]; then echo "ERROR: missing $TRAIN_CSV_PATH" >&2; exit 1; fi
if [ ! -f "$EVAL_CSV_PATH" ]; then echo "ERROR: missing $EVAL_CSV_PATH" >&2; exit 1; fi

TIMESTAMP=$(date +%d-%m-%y_%H:%M)
OUT_DIR=${HOME_DIR}/AI_lab_setup/Jobs/output/${SUBFOLDER}
ERR_DIR=${HOME_DIR}/AI_lab_setup/Jobs/error/${SUBFOLDER}
mkdir -p "$OUT_DIR" "$ERR_DIR"
exec > "${OUT_DIR}/cr01_${TIMESTAMP}_${RUN_TAG}_${SLURM_JOB_ID}.out" 2> "${ERR_DIR}/cr01_${TIMESTAMP}_${RUN_TAG}_${SLURM_JOB_ID}.err"

export PYTHONUNBUFFERED=1
source "${PROJECT_DIR}/venv/bin/activate"
cd "${HOME_DIR}/QDS/qds_v2"

RESULTS_DIR="${HOME_DIR}/QDS/qds_v2/results/${SUBFOLDER}/${RUN_TAG}_${SLURM_JOB_ID:-local}"
QB_RESULTS_DIR="${HOME_DIR}/QDS/qds_v2/results/${SUBFOLDER}_queryblind/${RUN_TAG}_${SLURM_JOB_ID:-local}"
SAVE_DIR="${HOME_DIR}/AISDATA/model"
GEN_EXP_DIR="${HOME_DIR}/AISDATA/generalized_experiment"
QUERY_DIR="${HOME_DIR}/AISDATA/Query_border_files"
ML_OUT_DIR="${HOME_DIR}/AISDATA/ML_processed_AIS_files"
mkdir -p "$RESULTS_DIR" "$QB_RESULTS_DIR" "$SAVE_DIR" "$GEN_EXP_DIR" "$QUERY_DIR" "$ML_OUT_DIR"

# ---- shared query/model settings (same as experiment) ----
N_QUERIES=50
QUERY_COVERAGE=0.30
MAX_QUERIES=2000
RANGE_SPATIAL_FRACTION=0.015
RANGE_TIME_FRACTION=0.0833
SIMILARITY_TIME_FRACTION=0.0833
KNN_T_HALF_WINDOW_FRACTION=0.0833
KNN_K=50
LR=0.0005
POINTWISE_LOSS_WEIGHT=0.25
GRADIENT_CLIP_NORM=1.0
EPOCHS=50
PATIENCE=50
DIAGNOSTIC_EVERY=5
DIAGNOSTIC_WINDOW_FRACTION=0.05
# F1 diagnostic ONLY at epoch 50 (start=50 + every=50 -> single fire at last epoch)
F1_DIAGNOSTIC_EVERY=50
F1_DIAGNOSTIC_START_EPOCH=50
TRAIN_BATCH_SIZE=128
CHECKPOINT_SELECTION_METRIC=uniform_gap
CHECKPOINT_UNIFORM_GAP_WEIGHT=0.5
CHECKPOINT_TYPE_PENALTY_WEIGHT=1.0
CHECKPOINT_SMOOTHING_WINDOW=5
CHECKPOINT_F1_VARIANT=answer
MLQDS_TEMPORAL_FRACTION=0.30
MLQDS_DIVERSITY_BONUS=0.05
RESIDUAL_LABEL_MODE="none"
SIMPLIFICATION_MODE="score_coverage"
COVERAGE_LAMBDA=0.5
COVERAGE_SIGMA_FRACTION=0.5
USE_CLS_TOKEN="true"
LENGTH_PRESERVATION_WEIGHT=2.0
COMPRESSION_RATIO=0.01

WORKLOAD="similarity"
TRAIN_MIX="similarity=1.0"
EVAL_MIX="similarity=1.0"

# Canonical checkpoint name in AISDATA/model (matches model_<workload>_cr<cr>.pt convention)
CHECKPOINT="${SAVE_DIR}/model_${WORKLOAD}_cr0p01.pt"
COVERAGE_TAG="${QUERY_COVERAGE/./p}"
RUN_ARTIFACT_TAG="${RUN_TAG}_${TRAIN_DATE_TAG}_eval_${EVAL_DATE_TAG}_cov${COVERAGE_TAG}_${SLURM_JOB_ID}"
# Query-blind eval log saved into generalized_experiment as <workload>_cr0p01.txt
GEN_EXP_FILE="${GEN_EXP_DIR}/${WORKLOAD}_cr0p01.txt"

# ============ PHASE 1: TRAIN at cr=0.01 (experiment settings) ============
python3 -u -X faulthandler -m src.experiments.run_ais_experiment \
    --train_csv_path "$TRAIN_CSV_PATH" \
    --eval_csv_path "$EVAL_CSV_PATH" \
    --n_queries "$N_QUERIES" \
    --query_coverage "$QUERY_COVERAGE" \
    --max_queries "$MAX_QUERIES" \
    --range_spatial_fraction "$RANGE_SPATIAL_FRACTION" \
    --range_time_fraction "$RANGE_TIME_FRACTION" \
    --knn_k "$KNN_K" \
    --knn_t_half_window_fraction "$KNN_T_HALF_WINDOW_FRACTION" \
    --similarity_time_fraction "$SIMILARITY_TIME_FRACTION" \
    --epochs "$EPOCHS" \
    --lr "$LR" \
    --pointwise_loss_weight "$POINTWISE_LOSS_WEIGHT" \
    --gradient_clip_norm "$GRADIENT_CLIP_NORM" \
    --diagnostic_every "$DIAGNOSTIC_EVERY" \
    --diagnostic_window_fraction "$DIAGNOSTIC_WINDOW_FRACTION" \
    --checkpoint_selection_metric "$CHECKPOINT_SELECTION_METRIC" \
    --f1_diagnostic_every "$F1_DIAGNOSTIC_EVERY" \
    --f1_diagnostic_start_epoch "$F1_DIAGNOSTIC_START_EPOCH" \
    --train_batch_size "$TRAIN_BATCH_SIZE" \
    --checkpoint_uniform_gap_weight "$CHECKPOINT_UNIFORM_GAP_WEIGHT" \
    --checkpoint_type_penalty_weight "$CHECKPOINT_TYPE_PENALTY_WEIGHT" \
    --checkpoint_smoothing_window "$CHECKPOINT_SMOOTHING_WINDOW" \
    --checkpoint_f1_variant "$CHECKPOINT_F1_VARIANT" \
    --mlqds_temporal_fraction "$MLQDS_TEMPORAL_FRACTION" \
    --mlqds_diversity_bonus "$MLQDS_DIVERSITY_BONUS" \
    --residual_label_mode "$RESIDUAL_LABEL_MODE" \
    --simplification_mode "$SIMPLIFICATION_MODE" \
    --coverage_lambda "$COVERAGE_LAMBDA" \
    --coverage_sigma_fraction "$COVERAGE_SIGMA_FRACTION" \
    --use_cls_token "$USE_CLS_TOKEN" \
    --length_preservation_weight "$LENGTH_PRESERVATION_WEIGHT" \
    --workload "$WORKLOAD" \
    --train_workload_mix "$TRAIN_MIX" \
    --eval_workload_mix "$EVAL_MIX" \
    --compression_ratio "$COMPRESSION_RATIO" \
    --model_type turn_aware \
    --early_stopping_patience "$PATIENCE" \
    --results_dir "$RESULTS_DIR" \
    --save_model "$CHECKPOINT" \
    --save_queries_dir "${QUERY_DIR}/run_${RUN_ARTIFACT_TAG}" \
    --save_simplified_dir "${ML_OUT_DIR}/run_${RUN_ARTIFACT_TAG}"

# ============ PHASE 2: QUERY-BLIND eval at cr=0.01 (--no_query_model), 02-10 ============
if [ ! -f "$CHECKPOINT" ]; then echo "ERROR: training did not produce $CHECKPOINT; skipping query-blind eval" >&2; exit 1; fi
python3 -u -X faulthandler -m src.experiments.run_inference \
    --checkpoint "$CHECKPOINT" \
    --csv_path "$EVAL_CSV_PATH" \
    --n_queries "$N_QUERIES" \
    --max_queries "$MAX_QUERIES" \
    --query_coverage "$QUERY_COVERAGE" \
    --range_spatial_fraction "$RANGE_SPATIAL_FRACTION" \
    --range_time_fraction "$RANGE_TIME_FRACTION" \
    --knn_k "$KNN_K" \
    --seed 42 \
    --workload_mix "$EVAL_MIX" \
    --compression_ratio "$COMPRESSION_RATIO" \
    --no_query_model \
    --results_dir "$QB_RESULTS_DIR" 2>&1 | tee "$GEN_EXP_FILE"
