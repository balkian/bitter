MAX_TAGS=100

function get_text {
    while read line
    do
        echo $line
        rtid=$(echo $line | awk '{print $2}')
        count=$(echo $line | awk '{print $1}')
        text=$(grep -m 1 $rtid *.text.csv)
        echo "$line - $text"
    done < "/dev/stdin"
}

cat "$@" | awk -F"," '{print tolower($2)}' | sort | uniq -c | sort -h | tail -n $MAX_TAGS | get_text

