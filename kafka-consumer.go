package main

import (
	"flag"
	"fmt"
	"os"
	"time"
	cluster "github.com/bsm/sarama-cluster"
	"strings"
	"unsafe"
	"log"
)


var debug bool

const rateRefresh = 60

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


	PrintDebug("Configuring the consumer")
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// Create new consumer
	consumer, err := cluster.NewConsumer(strings.Split(*param_bootstrap,","), *param_group, []string{*param_topic}, config)
	if err != nil {
		panic(err)
	}

	PrintDebug("Timing for",*param_duration)
	period := time.NewTimer(*param_duration)
	ticker := time.NewTicker(time.Second*rateRefresh)

	defer consumer.Close()

	rate := 0
	var num_msg uint64 = 0
	var total_size uint64 = 0

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			PrintDebug("Rebalanced: %+v\n", ntf)
		}
	}()

	PrintDebug("Here we go")
	// consume messages, watch signals
	for {
		select {
		case <-period.C:
			PrintDebug("TOC")
			consumer.Close()
		case <- ticker.C:
			fmt.Println("Current rate:",rate/rateRefresh,"messages per sec")
			rate = 0
		case msg := <- consumer.Messages():
			//PrintDebug(msg.Value)
			num_msg += 1
			rate += 1
			total_size += uint64(unsafe.Sizeof(msg.Value))
			consumer.MarkOffset(msg,"")
		}
	}

	consumer.Close()
	fmt.Println("Num:", num_msg, "Total Size:", total_size, "Average msg size:",total_size/num_msg)

}
