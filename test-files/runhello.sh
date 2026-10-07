#!/bin/sh
echo "Computing md5sum for localfile.txt";md5sum localfile.txt>>output.txt;
echo "Am sleeping for 30 seconds"
sleep 30
echo "I'm executing at $(pwd)"
exit 
