if [ "$#" -lt 1 ]
then
	echo "Usage: $0 <files to convert>"
	exit 1
fi

for i in "$@"
do
  REPLYOUTPUT=$i.replies.csv
  RTOUTPUT=$i.rts.csv
  echo 'created_at,id,user_id,reply_user_id' > $REPLYOUTPUT
  echo 'created_at,id,user_id,rt_user_id' > $RTOUTPUT
  pv -l -N "$i" $i | jq -r '. | select(.in_reply_to_user_id_str != null) | [.created_at, .id_str, .user.id_str, .in_reply_to_user_id_str] | @csv' >> $REPLYOUTPUT
  pv -l -N "$i" $i | jq -r '. | select(.retweeted_status != null) | [.created_at, .retweeted_status.id_str, .user.id_str, .retweeted_status.user.id_str] | @csv' >> $RTOUTPUT
done
