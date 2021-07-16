package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	cfg := kafka.ConfigMap{
		"bootstrap.servers":        "localhost",
		"group.id":                 "myGroup",
		"auto.offset.reset":        "earliest",
		"go.events.channel.enable": true,
		"enable.partition.eof":     true,
		"enable.auto.commit":       false,
		"auto.commit.interval.ms":  0,
	}
	if consumer, err := kafka.NewConsumer(&cfg); isOK(err) {
		if err := consumer.Subscribe("beesoft", nil); isOK(err) {
			defer consumer.Close()

			var wg sync.WaitGroup
			ctx, cancel := context.WithCancel(context.Background())
			osSignalChan := make(chan os.Signal)
			signal.Notify(osSignalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
			go func() {
				<-osSignalChan
				fmt.Printf("\n")
				cancel()
			}()

			wg.Add(1)
			go reader(ctx, &wg, consumer)
			wg.Wait()
		}
	}
	fmt.Println("Consumer finished.")
}

func reader(ctx context.Context, wg *sync.WaitGroup, consumer *kafka.Consumer) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Done")
			return
		case event := <-consumer.Events():
			go eventAnalyzer(event, consumer)
		}
	}
}

func eventAnalyzer(event kafka.Event, consumer *kafka.Consumer) {
	switch event.(type) {
	case *kafka.Message:
		message := event.(*kafka.Message)
		fmt.Printf("%v (%v[%d]@%d)\n",
			string(message.Value),
			*message.TopicPartition.Topic,
			message.TopicPartition.Partition,
			message.TopicPartition.Offset)
		if _, err := consumer.CommitMessage(message); err != nil {
			fmt.Println(err)
		}
	case kafka.OffsetsCommitted:
		fmt.Println("Offset commited")
	case kafka.Error:
		fmt.Println("Kafka error:\n\t", event.(kafka.Error))
	case kafka.PartitionEOF:
		pe := event.(kafka.PartitionEOF)
		fmt.Printf("End of partition (%v[%d]@%d)\n",
			*pe.Topic,
			pe.Partition,
			pe.Offset)
	default:
		fmt.Println("Unknown event:", event)
	}
}

func isOK(err error) bool {
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
}
