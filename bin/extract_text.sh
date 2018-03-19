if [ "$#" -lt 1 ]
then
	echo "Usage: $0 <files to convert>"
	exit 1
fi

QUERY='(.full_text // .retweeted_status.full_text) as $text | [ .id_str,$text ] | @csv'
HEADER='id,text'

for i in "$@"
do
  OUTPUT=$i.text.csv
  echo $HEADER > $OUTPUT
  pv -l -N "$i" $i | jq -r "$QUERY" >> $OUTPUT
 # sed -ri s/^"([0-9]+),\\"(.*)\\""$/"\1","\2"/g $OUTPUT
done
