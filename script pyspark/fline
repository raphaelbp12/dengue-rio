for f in *.csv; do
    tail -n +2 "$f" > "${f}".tmp && mv "${f}".tmp "$f"
    echo "Processing $f"
done
