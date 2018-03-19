MAX_TAGS=100

function get_text {
    while read line
    do
        echo $line
        rtid=$(echo $line | awk -F"," '{print $2}')
        text=$(grep -m 1 $rtid *.text.csv)
        echo "$line - $text"
    done < "/dev/stdin"
}

cat "$@" | get_text

