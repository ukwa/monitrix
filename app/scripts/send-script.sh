SCRIPT=$1 
src=`cat $SCRIPT`
value="$(perl -MURI::Escape -e 'print uri_escape($ARGV[0]);' "$src")"
echo "engine=groovy&script=$value" > temp.post
curl --data @temp.post -k -u u:p --anyauth --location -H "Accept: application/xml" "https://localhost:8443/engine/job/test/script"

