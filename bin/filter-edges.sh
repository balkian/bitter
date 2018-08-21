
if [ "$#" -lt 2 ]
then
    echo "Find edge lines in a file that contain one of the users in a user list."
    echo ""
	  echo "Usage: $0 <file with edges> <file with the list of users>"
	  exit 1
fi

pv -c -N 'read' "$1" |  grep -F -w -f "$2" |  pv -lc -N 'found'
