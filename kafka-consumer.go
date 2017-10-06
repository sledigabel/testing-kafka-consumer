package main

import (
	"flag"
	"fmt"
	"os"
	"time"
	"github.com/Shopify/sarama"
	"strings"
	"unsafe"
)


var num_msg []uint64
var total_size []uint64
var debug bool

func PrintDebug(s ...interface{}) {
	if debug {
		b := make([]interface{},len(s)+2)
		b[0] = "[DEBUG]"
		b[1] = time.Now().Format(time.RFC3339)
		for i, v := range(s){
			b[i+2] = v
		}
		fmt.Println(b...)
	}
}

func usage() {
	fmt.Printf("Usage: %s -bootstrap BOOTSTRAP [-rate MSG_RATE] [-duration DURATION] [-dryrun]\n\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func average(nums []uint64, sizes []uint64) uint64{
	var (
		n uint64
		s uint64
	)
	for i, v := range nums{
		n += v
		s += sizes[i]
	}
	return s / n
}

func main() {

	param_help := flag.Bool("help", false, "prints usage")
	param_bootstrap := flag.String("bootstrap", "", "Kafka bootstrap servers, typically localhost:9092")
	//param_quiet := flag.Bool("q", false, "quiet mode")
	param_group := flag.String("name", "kafka-consumer", "Consumer group for Kafka")
	param_topic := flag.String("topic","","topic to get msgs from")
	param_duration := flag.Duration("duration", time.Hour*24, "Receiving period")
	flag.BoolVar(&debug,"debug", false, "Debug mode")

	flag.Usage = usage
	flag.Parse()
	// var validation
	if *param_help {
		usage()
	}

	PrintDebug("Validating options")
	if len(*param_bootstrap) == 0 {
		fmt.Println("ERROR: parameter -bootstrap is mandatory")
		usage()
	}

	if len(*param_topic) == 0 {
		fmt.Println("ERROR: parameter -topic is mandatory")
		usage()
	}

	PrintDebug("Timing for",*param_duration)
	period := time.NewTimer(*param_duration)

	PrintDebug("Configuring the consumer")
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	sarama.Client()
	config.ClientID = *param_group
	config.

	// Create new consumer
	master, err := sarama.NewConsumer(strings.Split(*param_bootstrap,","), config)
	if err != nil {
		panic(err)
	}

	defer master.Close()
	var initialOffset int64 = sarama.OffsetOldest

	partitionList, err := master.Partitions(*param_topic)
	PrintDebug("Found partitions: ", partitionList)
	num_msg = make([]uint64,len(partitionList))
	total_size = make([]uint64,len(partitionList))
	closing := make(chan struct{})

	for id, partition := range partitionList {
		pc, err := master.ConsumePartition(*param_topic, partition, initialOffset)
		if err != nil {
			fmt.Printf("Failed to start consumer for partition %d: %s\n", partition, err)
			panic(err)
		}

		go func(pc sarama.PartitionConsumer,id int) {
			PrintDebug("Starting thread id", id)

			go func (pc sarama.PartitionConsumer, id int) {
				PrintDebug("handler thread id", id)
				<- closing
				pc.Close()
				PrintDebug("Closing thread id", id)
			}(pc,id)

			for message := range pc.Messages() {
				num_msg[id] += 1
				total_size[id] += uint64(unsafe.Sizeof(message))
				//PrintDebug("Update: ",id,num_msg[id],total_size[id])
			}
		}(pc,id)
	}

	select{
		case <- period.C:
			close(closing)
			break
	}
	for i, v := range(num_msg){
		fmt.Println("Partition #",i, "  Num:",v," Total Size:", total_size[i])
	}
	fmt.Println("Average msg size:",average(num_msg,total_size))

}
