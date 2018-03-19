if [ "$#" -lt 1 ]
then
	echo "Usage: $0 <files to convert>"
	exit 1
fi

export FIELDS="created_at,id,text" 
for i in "$@"
do
  OUTPUT=$i.hashtags.csv
  echo "$FIELDS" > $OUTPUT
  pv -l $i -N "hashtags $i" | jq -r '. | .created_at as $created_at | .id_str as $id | .entities.hashtags | select(. != null) | .[] | [$created_at, $id, .text] | @csv' >> $OUTPUT
done
