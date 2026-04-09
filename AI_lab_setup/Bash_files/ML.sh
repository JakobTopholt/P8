#!/bin/bash

#SBATCH --job-name=AIS_training_job
#SBATCH --output=/dev/null
#SBATCH --error=/dev/null
#SBATCH --mem=200G
#SBATCH --cpus-per-task=64
#SBATCH --gres=gpu:3
#SBATCH --time=04:00:00

# Redirect output/error to timestamped files
TIMESTAMP=$(date +%d-%m-%y_%H:%M)
OUT_DIR=/ceph/project/P8-1/AI_lab_setup/Jobs/output
ERR_DIR=/ceph/project/P8-1/AI_lab_setup/Jobs/error
exec > "${OUT_DIR}/training_AI_${TIMESTAMP}_${SLURM_JOB_ID}.out" 2> "${ERR_DIR}/training_AI_${TIMESTAMP}_${SLURM_JOB_ID}.err"

# Activate virtual environment
source /ceph/project/P8-1/venv/bin/activate

# Run  
cd /ceph/project/P8-1/QDS/qds_project
python3 -m src.experiments.run_ais_experiment --csv_path /ceph/project/P8-1/AISDATA/aisdk-2026-02-05.cleaned.csv/clean.csv --n_queries 5000 --epochs 50 --workload mixed --density_ratio 0.9 --compression_ratio 0.05 --dp_max_points 70000000 --importance_chunk_size 20000 --point_batch_size 5000 --model_type all --save_csv