package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	cfg := kafka.ConfigMap{
		"bootstrap.servers": "localhost",
	}

	if producer, err := kafka.NewProducer(&cfg); isOK(err) {
		defer producer.Close()

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
		go worker(ctx, &wg, producer)
		wg.Wait()

	}
}

func worker(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Producer) {
	defer wg.Done()

	msgCounter := 0

	msgChan := make(chan string)
	senderCtx, senderCancel := context.WithCancel(context.Background())
	var senderWg sync.WaitGroup
	senderWg.Add(1)
	go sender(senderCtx, &senderWg, producer, msgChan)

	for {
		select {
		case <-ctx.Done():
			senderCancel()
			senderWg.Wait()
			fmt.Println("Done")
			return

		// Wyświetlaj informację o wydarzeniach
		// zgłaszanych przez kafkę.
		case event := <-producer.Events():
			eventAnalyzer(event)

		// W regularnych odstępach czasu wysyłamy message
		// do kafki.
		case <-time.After(time.Millisecond * 500):
			msgChan<- fmt.Sprintf("message no %d", msgCounter)
			msgCounter += 1
		}
	}
}

func sender(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Producer, msgChan <-chan string) {
	defer wg.Done()

	topic := "beesoft"
	topicPartition := kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}

	for {
		select {
		case <-ctx.Done():
			producer.Flush(15 * 1000)
			fmt.Println("Done: sender.")
			return
		case text := <-msgChan:
			//partitionNumber := rand.Intn(2)
			//topicPartition := kafka.TopicPartition{Topic: &topic, Partition: int32(partitionNumber)}
			message := kafka.Message{TopicPartition: topicPartition, Value: []byte(text)}
			err := producer.Produce(&message, nil)
			isOK(err)
		}
	}
}

func eventAnalyzer(event kafka.Event) {
	switch ev := event.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed (%v)\n", ev.TopicPartition)
		} else {
			fmt.Printf("Message delivered (%v)\n", ev.TopicPartition)
		}
		//message := event.(*kafka.Message)
		//fmt.Println("message event:", message)
	default:
		fmt.Printf("Unknown event: %v\n", event)
	}
}

func isOK(err error) bool {
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
}
