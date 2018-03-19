cat "$@" | awk -F"," '{print tolower($3)}' | sort | uniq -c | sort -h 
