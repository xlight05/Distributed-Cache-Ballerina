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
while [ $counter -le 3 ]
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
gnome-terminal -e "ballerina run Client.bal -e port=$port -e ip="http://localhost" -e id=$id -e api=$api -e hosts=$lastHost"
#gnome-terminal -e echo "helloo"
#echo "Helloo";  
lastHost=$lastHost","$urlWithPort
    ((port++))
    ((counter++))
    ((api++))
done

# myString="abcd"
# myString="${myString:1}"
# echo $myString