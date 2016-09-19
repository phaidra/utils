#!/bin/bash

  usage="To stop the script nicely, delete the volare_upload.pid file. Set environment variables VOLARE_UPLOAD_FOLDER, VOLARE_UPLOAD_USERNAME, VOLARE_UPLOAD_PASSWORD. Usage: volare_upload.pl"

  FOLDER=$VOLARE_UPLOAD_FOLDER
  USERNAME=$VOLARE_UPLOAD_USERNAME
  PASSWORD=$VOLARE_UPLOAD_PASSWORD

  if [ ! -d "$FOLDER" ] 
  then
    echo "$FOLDER is not a folder."
    echo "$usage"
    exit 1
  fi

  echo $$ > $FOLDER/volare_upload.pid

  if [ ! -f "$FOLDER/volare_upload.pid" ]
  then
    echo "Cannot create $FOLDER/volare_upload.pid file."
    echo "$usage"
    exit 1
  fi

  if [ -z $USERNAME ]
  then
    echo "VOLARE_UPLOAD_USERNAME is not set."
    echo "$usage"
    exit 1  
  fi
  if [ -z $PASSWORD ]
  then
    echo "VOLARE_UPLOAD_PASSWORD is not set."
    echo "$usage"
    exit 1
  fi

  shopt -s nullglob
  array=($FOLDER/*.tif)
  #printf '%s\n' "${array[@]}"

  tLen=${#array[@]}
  for (( i=0; i<${tLen}; i++ ));
  do 
    if [ ! -f "$FOLDER/volare_upload.pid" ]
    then
      echo "Cannot find $FOLDER/volare_upload.pid file. Stopping..."
      exit 0
    fi
    echo "uploading ${array[$i]} [$(($i+1))/${#array[@]}]" 
    curlCMD="curl -X POST -u $USERNAME:$PASSWORD --write-out %{http_code} --silent --output /dev/null -F "mimetype=image/tiff" -F "file=@${array[$i]}" -F "metadata=@${array[$i]}.json" http://127.0.0.1:3001/picture/create"
    #echo $curlCMD
    res="$($curlCMD)"
    #echo $res
    if [ $res -eq '200' ]
    then
      echo "success"
      rm ${array[$i]}
      rm ${array[$i]}.json
    else
      echo "failed ($res)"
      exit 1
    fi
  done

  rm  $FOLDER/volare_upload.pid
  exit 0
  

