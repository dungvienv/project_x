#! /bin/bash

# Check if docker-compose stack was available

if [ -n "$(docker-compose ps --all --services)" ];then
    echo "docker-compose stack is already there. Using docker-compose start."
    docker-compose start 
else 
    echo "docker-compose stack cannot found. Using docker-compose up."
    docker-compose up -d --build
fi