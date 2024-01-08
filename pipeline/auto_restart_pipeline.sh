# bash script to run historical pipeline until completion, restarting when it crashes

hist_pipeline(){
    python historical_pipeline.py -d "2024-01-06"
}

rm readings.csv
rm historical_pipeline_log.txt

until hist_pipeline; do
    echo "'historical_pipeline.py' crashed with exit code $?. Restarting pipeline..." >&2
    sleep 5
done

