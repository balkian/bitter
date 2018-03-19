if [ "$#" -lt 1 ]
then
	echo "Usage: $0 <files to convert>"
	exit 1
fi

QUERY='.| select(.retweeted_status != null) | .retweeted_status | .id_str as $rt_id | .extended_tweet | select(. != null) | [$rt_id,.full_text]|@csv'
HEADER='rt_id,full_text'

for i in "$@"
do
  OUTPUT=$i.full_text.csv
  echo $HEADER > $OUTPUT
  jq "$QUERY" $i | pv -N "$i" -l >> $OUTPUT
  sort -u $OUTPUT -o $OUTPUT
  sed -ri 's/^"([0-9]+),\\"(.*)\\""$/"\1","\2"/g' $OUTPUT
done
