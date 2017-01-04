package main

import (
	"flag"
	"fmt"
	"log"
	"time"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/gocql/gocql"

	common "app/common"
	user "app/user"
)

var (
	brokerList = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topic      = flag.String("topic", os.Getenv("KAFKA_TOPICS"), "REQUIRED: the topic to consume")
	partitions = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	offset     = flag.String("offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
	verbose    = flag.Bool("verbose", false, "Whether to turn on sarama logging")
	bufferSize = flag.Int("buffer-size", 1 /*os.Getenv("KAFKA_BUFFER_SIZE")*/, "The buffer size of the message channel.")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)
//127.0.0.1:82/user/create?data={"version": 1, "data": {"login": "olegabr","password": "123456","name": "Oleg Abrosimov","avatarUrls": ["url/test","url2/test"],"emails": ["olegabr@mail.ru", "olegabrosimovnsk@gmail.com"],"phones": ["2342342134", "34234234523"],"latitude": "123","longitude": "321","socialCodes": {"fb": "_f6s5d765ds75fa67s5da6sd"},"birthdate": "1472781728000"}}

type Message struct {
	Version json.Number `json:"version"`
	Data user.User `json:"data"`
}

func main() {
	flag.Parse()

	if *brokerList == "" {
		printUsageErrorAndExit("You have to provide -brokers as a comma-separated list, or set the KAFKA_PEERS environment variable.")
	}

	if *topic == "" {
		printUsageErrorAndExit("-topic is required")
	}

	if *verbose {
		sarama.Logger = logger
	}

	var initialOffset int64
	switch *offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	// connect to the cluster
	cluster := gocql.NewCluster(os.Getenv("CASSANDRA_PEERS"))
	cluster.Keyspace = os.Getenv("CASSANDRA_KEYSPACE")
	var cons = os.Getenv("CASSANDRA_CONSISTENCY")
	kafka_partition0, _ := strconv.Atoi(os.Getenv("KAFKA_PARTITION"))
	kafka_partition := int32(kafka_partition0)
	kafka_consumer_group := os.Getenv("KAFKA_CONSUMER_GROUP")
	if len(cons) == 0 {
		cons = "ONE"
	}
	cluster.Consistency = gocql.ParseConsistency(cons)
	var err error;
	cluster.ProtoVersion, err = strconv.Atoi(os.Getenv("CASSANDRA_PROTOCOL_VERSION"))
	if err != nil {
		printErrorAndExit(69, "Failed to get cassandra protocol version from CASSANDRA_PROTOCOL_VERSION: %s", err)
	}
	var timeout int
	timeout, err = strconv.Atoi(os.Getenv("CASSANDRA_CONNECTION_TIMEOUT"))
	if err != nil {
		printErrorAndExit(69, "Failed to get cassandra connection timeout from CASSANDRA_CONNECTION_TIMEOUT: %s", err)
	}
	cluster.Timeout = time.Duration(timeout) * time.Millisecond
	session, err := cluster.CreateSession()
	if err != nil {
		printErrorAndExit(69, "Failed to get cassandra session: %s", err)
	}
	defer session.Close()

	client, err := sarama.NewClient(strings.Split(*brokerList, ","), nil)
	if err != nil {
		printErrorAndExit(69, "Failed to create client: %s", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			logger.Println("Failed to close client: ", err)
		}
	}()

	// https://github.com/Shopify/sarama/commit/f0062da5ae1a62f7841e563a244e11a5ad9b96d7
	config := client.Config()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	c, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		printErrorAndExit(69, "Failed to create consumer: %s", err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			logger.Println("Failed to close consumer: ", err)
		}
	}()

	om, err := sarama.NewOffsetManagerFromClient(kafka_consumer_group, client)
	if err != nil {
		printErrorAndExit(69, "Failed to create offset manager: %s", err)
	}
	defer func() {
		if err := om.Close(); err != nil {
			logger.Println("Failed to close om: ", err)
		}
	}()

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		printErrorAndExit(69, "Failed to create producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partitionList, err := getPartitions(c)
	if err != nil {
		printErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}
	found := false
	for _, partition := range partitionList {
		fmt.Printf("Found partition:\t%d\n", partition)
		if kafka_partition == partition {
			found = true
		}
	}
	if false == found {
		printErrorAndExit(69, "Partition is not found")
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, *bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)
	defer func() {
		close(messages)
	}()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	pom, err := om.ManagePartition(*topic, kafka_partition)
	if err != nil {
		printErrorAndExit(69, "Failed to manage partition %d: %s", kafka_partition, err)
	}
	defer func() {
		if err := pom.Close(); err != nil {
			logger.Println("Failed to close pom: ", err)
		}
	}()
	offset, meta := pom.NextOffset()
	if "" == meta {
		// первый вызов
		offset = initialOffset
	}
	fmt.Printf("Found Offset:\t%d\n", offset)

	pc, err := c.ConsumePartition(*topic, kafka_partition, offset)
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer for partition %d: %s", kafka_partition, err)
	}

	go func(pc sarama.PartitionConsumer) {
		<-closing
		pc.AsyncClose()
	}(pc)

	wg.Add(1)
	go func(pc sarama.PartitionConsumer) {
		// никогда сами не выходим. лишь смерть разлучит нас.
		//defer wg.Done()
		for message := range pc.Messages() {
			messages <- message
		}
	}(pc)

	go func() {
		for msg := range messages {
			fmt.Printf("Partition:\t%d\n", msg.Partition)
			fmt.Printf("Offset:\t%d\n", msg.Offset)
			fmt.Printf("Key:\t%s\n", string(msg.Key))

			data := string(msg.Value)
			fmt.Printf("Value:\t%s\n", data)

			fmt.Println()

			var m Message
			if err := json.Unmarshal(msg.Value, &m); err != nil {
				logger.Println("Failed to Unmarshal message value: ", err)
				// nonempty - чтобы первая проверка сработала
				pom.MarkOffset(msg.Offset + 1, "nonempty")
				logger.Println("Set new offset value: ", msg.Offset + 1)
				continue;
			}

			fmt.Printf("Version:\t%d\n", m.Version)
			//fmt.Printf("Data:\t%s\n", string(m.Data))

			//s, _ := strconv.Unquote(string(m.Data))

			var u *user.User = &m.Data
			//if err := json.Unmarshal([]byte(s), &u); err != nil {
			//	logger.Println("Failed to Unmarshal message data value: ", err)
			//	return
			//}

			res2B, _ := json.Marshal(u)
			logger.Println("Unmarshalled user result: ", string(res2B))

			if "" == u.Login  || 
				"" == u.Password  || 
				"" == u.Name || 
				"" == u.Birthdate   || 
				0 == len(u.Emails) {
				logger.Println("Bad arguments supplied: login, password, name, birthdate and emails are required");
				// nonempty - чтобы первая проверка сработала
				pom.MarkOffset(msg.Offset + 1, "nonempty")
				logger.Println("Set new offset value: ", msg.Offset + 1)
				continue;
			}

			if "" == u.State {
				u.SetState(common.UserStateNew);
			}

			if err = u.Insert(session, producer, &data); nil != err {
				logger.Println("Failed to create user: ", err);
				// TODO: скинуть задачу в отдельную очередь
				//logger.Println("Failed to create user: ", err);
			}
			// задание выполнено
			// nonempty - чтобы первая проверка сработала
			pom.MarkOffset(msg.Offset + 1, "nonempty")
			logger.Println("Set new offset value: ", msg.Offset + 1)
		}
	}()

	wg.Wait()
	logger.Println("Done consuming topic", *topic)
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	if *partitions == "all" {
		return c.Partitions(*topic)
	}

	tmp := strings.Split(*partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}
