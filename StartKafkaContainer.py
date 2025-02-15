import docker

client = docker.from_env()

# Pull Kafka and Zookeeper images
client.images.pull("bitnami/zookeeper")
client.images.pull("bitnami/kafka")

# Start Zookeeper
zookeeper_container = client.containers.run(
    "bitnami/zookeeper",
    name="zookeeper",
    detach=True,
    environment={"ALLOW_ANONYMOUS_LOGIN": "yes"},
    ports={"2181/tcp": 2181}
)

# Start Kafka
kafka_container = client.containers.run(
    "bitnami/kafka",
    name="kafka",
    detach=True,
    environment={
        "KAFKA_CFG_ZOOKEEPER_CONNECT": "zookeeper:2181",
        "ALLOW_PLAINTEXT_LISTENER": "yes"
    },
    ports={"9092/tcp": 9092},
    links=["zookeeper"]
)

print("Kafka and Zookeeper are running!")
