if [ "$#" -lt 1 ]
then
	echo "Usage: $0 <files to convert>"
	exit 1
fi

FIELDS=".id_str,\
        .user.screen_name,\
        .user.id,\
        .favorite_count,\
        .retweet_count,\
        .quote_count,\
        .reply_count,\
        .created_at,\
        .lang,\
        .in_reply_to_user_id_str,\
        .in_reply_to_status_id_str,\
        .retweeted_status.id_str,\
        .retweeted_status.user.id,\
        .retweeted_status.favorite_count,\
        .retweeted_status.retweet_count,\
        .retweeted_status.quote_count,\
        .retweeted_status.reply_count,\
        .retweeted_status.created_at\
"

for i in "$@"
do
  OUTPUT=$i.tweets.csv
  echo "$FIELDS" | sed -e 's/,[ \t\n]*\./,/g' | sed -e 's/^[#]\?\.//' > $OUTPUT
  jq -r "[$FIELDS]|@csv" $i | pv -N "$i" -l >> $OUTPUT
done
