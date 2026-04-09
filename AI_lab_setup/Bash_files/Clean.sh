#!/bin/bash

#SBATCH --job-name=AIS_cleaning_job
#SBATCH --output=/dev/null
#SBATCH --error=/dev/null
#SBATCH --mem=128G
#SBATCH --cpus-per-task=64
#SBATCH --gres=gpu:1
#SBATCH --time=04:00:00

# Redirect output/error to timestamped files
TIMESTAMP=$(date +%d-%m-%y_%H:%M)
OUT_DIR=/ceph/project/P8-1/AI_lab_setup/Jobs/output
ERR_DIR=/ceph/project/P8-1/AI_lab_setup/Jobs/error
exec > "${OUT_DIR}/cleaning_${TIMESTAMP}_${SLURM_JOB_ID}.out" 2> "${ERR_DIR}/cleaning_${TIMESTAMP}_${SLURM_JOB_ID}.err"

# Activate virtual environment
source /ceph/project/P8-1/venv/bin/activate

# Set input file path (raw CSVs live under raw_AIS_files/)
export AIS_INPUT_FILE=/ceph/project/P8-1/AISDATA/raw_AIS_files/aisdk-2026-02-05.csv

# Run  
cd /ceph/project/P8-1
/ceph/project/P8-1/venv/bin/python3 main.py