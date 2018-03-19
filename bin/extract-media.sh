if [ "$#" -lt 1 ]
then
	echo "Usage: $0 <files to convert>"
	exit 1
fi

export QUERY='select(.id != null) | .id_str as $id | .entities.urls[] | select(.expanded_url | select(. != null) |  contains("open.spotify") or contains("youtube.com") or contains("youtu.be")) | [$id, .expanded_url] | @csv'

export FIELDS="id,url"

for i in "$@"
do
  OUTPUT=$i.media.csv
  echo $FIELDS > $OUTPUT
  pv -N "$i media" -l $i | jq -r "$QUERY" >> $OUTPUT
done
