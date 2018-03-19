if [ "$#" -lt 1 ]
then
	echo "Usage: $0 <files to convert>"
	exit 1
fi

export QUERY='.limit | select(. != null) | [.timestamp_ms, .track] | @csv'

export FIELDS="timestamp,track"

for i in "$@"
do
  OUTPUT=$i.limits.csv
  echo $FIELDS > $OUTPUT
  pv -N "$i limits" -l $i | jq -r "$QUERY" >> $OUTPUT
done
