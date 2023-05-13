"""
docker build -t group_leader_image -f docker/GroupCoordinatorDockerFile .
docker run -it group_leader_image consumer_group_name=new_group

docker build -t consumer_image -f docker/ConsumerDockerFile .
docker run -e PYTHONUNBUFFERED=1  consumer_image consumer_group_name=new_group

docker build -t broker_client_image -f docker/BrokerClientDockerFile .
docker run -it broker_client_image

"""
import sys
