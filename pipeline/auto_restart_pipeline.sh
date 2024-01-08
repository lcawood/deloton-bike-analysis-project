# bash script to run historical pipeline until completion, restarting when it crashes

hist_pipeline(){
    python historical_pipeline.py -d "2024-01-06"
}

until hist_pipeline; do
    echo "'historical_pipeline.py' crashed with exit code $?. Restarting pipeline..." >&2
    sleep 1
done

