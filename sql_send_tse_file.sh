# Mettre ":i:0:"
while getopts ":i:" option
do
case "${option}"
in
i) SearchDest=${OPTARG};;
esac
done

# Check script input integrity
if [[ $SearchDest ]];
then
  echo "Start loading processing"
else
  echo "No Search directory: $SearchDest or Target directory: $TargetDest, use -i options" >&2
  exit 1
fi


#Hack to force the bash to split command result only on newline char
#It is done to support the spaces in the folder names
OIFS="$IFS"
IFS=$'\n'

for TSE_FILE in $(find $SearchDest* -type f -name "*.tse_bi" ); 
do

    python3 sql_send_tse_file.py -t $TSE_FILE

    if [ $? -eq 0 ]
    then
        echo "File $TSE_FILE processed"
    else
        echo "Failed to process $TSE_FILE" >&2
    fi
done

echo "all tse files processed"

IFS="$IFS"
exit 0