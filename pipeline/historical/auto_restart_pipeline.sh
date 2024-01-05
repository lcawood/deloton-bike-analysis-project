
pipeline(){
    python pipeline.py
}

until pipeline; do
    echo "'pipeline.py' crashed with exit code $?. Restarting pipeline..." >&2
    sleep 1
done

