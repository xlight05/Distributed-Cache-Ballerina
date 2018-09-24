#!/bin/bash
# Basic while loop
# counter=1
# while [ $counter -le 10 ]
# do
#     echo $counter
#     ((counter++))
# done

port=2000
api=7000
counter=1
lastHost=""
#echo "Total Nodes?"
#totalNodes=read
while [ $counter -le 2 ]
do
url="http://localhost"
urlWithPort=$url":"$port
firstChar=${lastHost:0:1}
if [ "$firstChar" = "," ]
then
     lastHost="${lastHost:1}"
fi
#echo $lastHost

##Open in a new window
#ballerina run Client.bal -e port=$port -e ip="http://localhost" -e id=$id -e api=$api -e hosts=$lastHost
gnome-terminal -e "ballerina run member.bal -e port=$port -e ip="http://localhost" -e id=$counter -e api=$api -e hosts=$lastHost"
lastHost=$lastHost","$urlWithPort
    ((port++))
    ((counter++))
    ((api++))
done
gnome-terminal -e "ballerina run put_bench.bal -e port=$port -e ip="http://localhost" -e id=$counter -e api=$api -e hosts=$lastHost"

# myString="abcd"
# myString="${myString:1}"
# echo $myString