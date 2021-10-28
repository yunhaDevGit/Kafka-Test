# Kafka-Test
Spring Boot 와 Kafka 연동 예제입니다.
Docker를 사용하여 kafka 설치 및 실행 후 Spring Boot와 연동하였습니다. 


## Kafka 설치 및 실행 (with Docker)

### 1\. docker 이미지 선택

```
docker search kafka
```

docker search 명령어를 통해 docker 이미지 파일을 검색합니다.

![](https://blog.kakaocdn.net/dn/rbFUf/btrikLFikwj/9yvqmZLvKRNmekd3f73ZCK/img.png)

가장 star이 많은 이미지를 선택하겠습니다.

### 2\. docker-compose.yml 파일 작성

kafka는 항상 zookeeper와 함께 동작해야 하므로 여러 docker container를 실행하기 편리한 docker-compose를 사용하여 구성합니다.

**docker-compose.yml**

```
version: '2'
services:
  zookeeper:
    image: wurstmeister/zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
  kafka:
    image: wurstmeister/kafka:2.12-2.5.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_ADVERTISED_HOST_NAME: 127.0.0.1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

docker-compose.yml 파일 작성이 완료 되었다면, docker-compose.yml을 실행시킵니다.

```
docker-compose up -d
```

![](https://blog.kakaocdn.net/dn/OjePi/btrikLrD0jT/xhkEIdGmSmnl9X54EfE180/img.png)


### 3\. Kafka 바이너리 파일 다운로드

Kafka가 제대로 실행되었는지 확인하기 위해 공식 사이트에서 제공하는 kafka 바이너리 파일들을 다운로드 합니다. 

**여기서 주의할 점은 kafka 컨테이너의 실행 이미지 버전과 kafka 바이너리 파일의 버전이 동일해야 합니다.**


_kafka 바이러니 파일 다운로드 경로_

[https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)



```
# kafka 바이너리 파일 압축 풀 경로로 이동

# 바이너리 파일 다운로드
wget https://archive.apache.org/dist/kafka/2.5.0/kafka_2.12-2.5.0.tgz

# 압축 풀기
tar -zxvf kafka_2.12-2.5.0.tgz
```
![](https://blog.kakaocdn.net/dn/by0KZv/btrinhQ15Yn/oU8KJbyL1dXMziPpkaqoIK/img.png)
![](https://blog.kakaocdn.net/dn/c0k8K6/btrilU2ZA1E/NgLcavylfQlUitbcLwAwGk/img.png)


### 4\. Topic 생성 및 조회, 삭제

먼저 kafka 컨테이너에 topic을 만들어 보겠습니다. 

```
bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
```

![](https://blog.kakaocdn.net/dn/xgzv0/btrinAbM1cK/ZVFiqlgI1DuHZUC8Fk7AZk/img.png)

-   \--create : 새로운 토픽을 만들 때 사용하는 옵션
-   \--bootstarp-server : 연결할 Kafka 서버 (host:port)  
                                   이 옵션 추가 시, 직접 zookeeper에 연결하지 않아도 됩니다.

아래의 명령어를 통해 생성한 Topic의 상세 정보를 확인 할 수 있습니다.

```
bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092
```
![](https://blog.kakaocdn.net/dn/4FkI8/btrinsrrZla/NYz0bOXfD29k3YwJ9RBwBk/img.png)

-   \--describe : 운영 상에 필요한 Topic의 상세 정보를 보여줍니다.

생성한 topic을 삭제하고 싶을 경우 아래와 같이 --delete 옵션을 사용하여 삭제할 수 있습니다.



```
bin/kafka-topics.sh --delete --topic quickstart-events --bootstrap-server localhost:9092
```
![](https://blog.kakaocdn.net/dn/CMfPG/btriq7N1iz1/5IH85Z1JnVMSDkg5vKIVh1/img.png)

-   \--delete : topic을 삭제학 위해 server.properties 파일에서 delete.topic.enable=true 설정을 추가해줘야 합니다.

### 5\. Producer, Consumer 옵션 확인 및 메시지 생산/소비

**Producer 관련 옵션 확인**

```
bin/kafka-console-producer.sh --help
```

Topic에 메시지를 보냅니다. (메시지 생산)

```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
> hello kafka
> test kafka
```

**Consumer 관련 옵션 확인**

```
bin/kafka-console-consumer.sh --help
```

새로운 터미널을 열어 생산된 메시지를 확인해봅니다. (메시지 소비)

```
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

-   \--from-beginning : Consumer에게 설정된 offset이 없으므로 가장 최신의 메시지 대신 먼저 도착한 메시지부터 읽도록 하는 옵션입니다.

producer에서 메시지를 보내면 Consumer에서 바로 확인이 가능합니다.

![](https://blog.kakaocdn.net/dn/cpmGrE/btrioG4jli9/fGwXVhBpwBEwOEBDokenRK/img.png)


## Kafka와 Spring boot 연동
### Spring Boot project 생성

IDE를 사용하여 새로운 spring boot 프로젝트를 생성합니다.

### dependency 설정

Spring Boot 프로젝트가 준비 되었다면 **build.gradle**을 아래와 같이 수정하여 spring-kafka에 대한 의존성을 추가해줍니다.

```
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter'
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    implementation 'org.springframework.boot:spring-boot-starter-web'

    implementation 'org.springframework.kafka:spring-kafka'
    
}
```
