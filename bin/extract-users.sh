if [ "$#" -lt 1 ]
then
	echo "Usage: $0 <files to convert>"
	exit 1
fi

export USER_FIELDS="\$created_at,\
.id_str,\
.screen_name,\
.followers_count,\
.lang,\
.description,\
.statuses_count,\
.favourites_count,\
.friends_count,\
.created_at,\
.name,\
.location,\
.listed_count,\
.time_zone\
"

for i in "$@"
do
  OUTPUT=$i.users.csv
  echo \#$USER_FIELDS > $OUTPUT
  jq -r ".created_at as \$created_at | .user,.retweeted_status.user | select(. != null) | [$USER_FIELDS] | @csv " $i | pv -N "$i" -l >> $OUTPUT
done
