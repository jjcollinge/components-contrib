// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicebus

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	azservicebus "github.com/Azure/azure-service-bus-go"
	"github.com/Azure/go-shuttle/listener"
	"github.com/Azure/go-shuttle/message"
	"github.com/Azure/go-shuttle/publisher"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	// Keys
	connectionString               = "connectionString"
	consumerID                     = "consumerID"
	maxDeliveryCount               = "maxDeliveryCount"
	timeoutInSec                   = "timeoutInSec"
	lockDurationInSec              = "lockDurationInSec"
	lockRenewalInSec               = "lockRenewalInSec"
	defaultMessageTimeToLiveInSec  = "defaultMessageTimeToLiveInSec"
	autoDeleteOnIdleInSec          = "autoDeleteOnIdleInSec"
	disableEntityManagement        = "disableEntityManagement"
	maxConcurrentHandlers          = "maxConcurrentHandlers"
	handlerTimeoutInSec            = "handlerTimeoutInSec"
	prefetchCount                  = "prefetchCount"
	maxActiveMessages              = "maxActiveMessages"
	maxActiveMessagesRecoveryInSec = "maxActiveMessagesRecoveryInSec"
	errorMessagePrefix             = "azure service bus error:"

	// Defaults
	defaultTimeoutInSec        = 60
	defaultHandlerTimeoutInSec = 60
	defaultLockRenewalInSec    = 20
	// ASB Messages can be up to 256Kb. 10000 messages at this size would roughly use 2.56Gb.
	// We should change this if performance testing suggests a more sensible default.
	defaultMaxActiveMessages              = 10000
	defaultMaxActiveMessagesRecoveryInSec = 2
	defaultDisableEntityManagement        = false

	maxReconnAttempts       = 10
	connectionRecoveryInSec = 2
)

type handler = struct{}

type azureServiceBus struct {
	metadata      metadata
	listener      *listener.Listener
	logger        logger.Logger
	features      []pubsub.Feature
	publishers    map[string]*publisher.Publisher
	publisherLock *sync.RWMutex
}

// NewAzureServiceBus returns a new Azure ServiceBus pub-sub implementation
func NewAzureServiceBus(logger logger.Logger) pubsub.PubSub {
	return &azureServiceBus{
		logger:        logger,
		features:      []pubsub.Feature{pubsub.FeatureMessageTTL},
		publishers:    map[string]*publisher.Publisher{},
		publisherLock: &sync.RWMutex{},
	}
}

func parseAzureServiceBusMetadata(meta pubsub.Metadata) (metadata, error) {
	m := metadata{}

	/* Required configuration settings - no defaults */
	if val, ok := meta.Properties[connectionString]; ok && val != "" {
		m.ConnectionString = val
	} else {
		return m, fmt.Errorf("%s missing connection string", errorMessagePrefix)
	}

	if val, ok := meta.Properties[consumerID]; ok && val != "" {
		m.ConsumerID = val
	} else {
		return m, fmt.Errorf("%s missing consumerID", errorMessagePrefix)
	}

	/* Optional configuration settings - defaults will be set by the client */
	m.TimeoutInSec = defaultTimeoutInSec
	if val, ok := meta.Properties[timeoutInSec]; ok && val != "" {
		var err error
		m.TimeoutInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid timeoutInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.DisableEntityManagement = defaultDisableEntityManagement
	if val, ok := meta.Properties[disableEntityManagement]; ok && val != "" {
		var err error
		m.DisableEntityManagement, err = strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid disableEntityManagement %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.HandlerTimeoutInSec = defaultHandlerTimeoutInSec
	if val, ok := meta.Properties[handlerTimeoutInSec]; ok && val != "" {
		var err error
		m.HandlerTimeoutInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid handlerTimeoutInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.LockRenewalInSec = defaultLockRenewalInSec
	if val, ok := meta.Properties[lockRenewalInSec]; ok && val != "" {
		var err error
		m.LockRenewalInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid lockRenewalInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxActiveMessages = defaultMaxActiveMessages
	if val, ok := meta.Properties[maxActiveMessages]; ok && val != "" {
		var err error
		m.MaxActiveMessages, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxActiveMessages %s, %s", errorMessagePrefix, val, err)
		}
	}

	m.MaxActiveMessagesRecoveryInSec = defaultMaxActiveMessagesRecoveryInSec
	if val, ok := meta.Properties[maxActiveMessagesRecoveryInSec]; ok && val != "" {
		var err error
		m.MaxActiveMessagesRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid recoveryInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	/* Nullable configuration settings - defaults will be set by the server */
	if val, ok := meta.Properties[maxDeliveryCount]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxDeliveryCount %s, %s", errorMessagePrefix, val, err)
		}
		m.MaxDeliveryCount = &valAsInt
	}

	if val, ok := meta.Properties[lockDurationInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid lockDurationInSec %s, %s", errorMessagePrefix, val, err)
		}
		m.LockDurationInSec = &valAsInt
	}

	if val, ok := meta.Properties[defaultMessageTimeToLiveInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid defaultMessageTimeToLiveInSec %s, %s", errorMessagePrefix, val, err)
		}
		m.DefaultMessageTimeToLiveInSec = &valAsInt
	}

	if val, ok := meta.Properties[autoDeleteOnIdleInSec]; ok && val != "" {
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid autoDeleteOnIdleInSecKey %s, %s", errorMessagePrefix, val, err)
		}
		m.AutoDeleteOnIdleInSec = &valAsInt
	}

	if val, ok := meta.Properties[maxConcurrentHandlers]; ok && val != "" {
		var err error
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid maxConcurrentHandlers %s, %s", errorMessagePrefix, val, err)
		}
		m.MaxConcurrentHandlers = &valAsInt
	}

	if val, ok := meta.Properties[prefetchCount]; ok && val != "" {
		var err error
		valAsInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("%s invalid prefetchCount %s, %s", errorMessagePrefix, val, err)
		}
		m.PrefetchCount = &valAsInt
	}

	return m, nil
}

func (a *azureServiceBus) Init(metadata pubsub.Metadata) error {
	m, err := parseAzureServiceBusMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = m

	return nil
}

func (a *azureServiceBus) Publish(req *pubsub.PublishRequest) error {
	var sender *publisher.Publisher
	var err error

	a.publisherLock.RLock()
	if p, ok := a.publishers[req.Topic]; ok {
		sender = p
	}
	a.publisherLock.RUnlock()

	if sender == nil {
		a.publisherLock.Lock()
		sender, err := publisher.New(req.Topic, publisher.WithConnectionString(a.metadata.ConnectionString))
		a.publishers[req.Topic] = sender
		a.publisherLock.Unlock()

		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()

	sbMsg := azservicebus.NewMessage(req.Data)
	ttl, hasTTL, _ := contrib_metadata.TryGetTTL(req.Metadata)
	if hasTTL {
		sbMsg.TTL = &ttl
	}

	msg, err := message.New(sbMsg)
	if err != nil {
		return err
	}

	return sender.Publish(ctx, msg)
}

func (a *azureServiceBus) Subscribe(req pubsub.SubscribeRequest, appHandler func(msg *pubsub.NewMessage) error) error {
	var err error
	a.listener, err = listener.New(
		listener.WithConnectionString(a.metadata.ConnectionString),
		listener.WithSubscriptionName(a.metadata.ConsumerID),
		listener.WithSubscriptionDetails(time.Second*time.Duration(*a.metadata.LockDurationInSec), int32(*a.metadata.MaxDeliveryCount)))
	if err != nil {
		return err
	}

	handler := message.HandleFunc(func(ctx context.Context, msg *message.Message) message.Handler {
		m := &pubsub.NewMessage{
			Data:  []byte(msg.Data()),
			Topic: req.Topic,
		}
		err := appHandler(m)
		if err != nil {
			return msg.Error(err)
		}
		return msg.Complete()
	})

	return a.listener.Listen(
		context.Background(),
		handler,
		req.Topic,
		listener.WithMaxConcurrency(*a.metadata.MaxConcurrentHandlers),
		listener.WithMessageLockAutoRenewal(time.Duration(a.metadata.LockRenewalInSec)),
		listener.WithPrefetchCount(uint32(*a.metadata.PrefetchCount)))
}

func (a *azureServiceBus) Close() error {
	defer a.listener.Close(context.Background())

	return nil
}

func (a *azureServiceBus) Features() []pubsub.Feature {
	return a.features
}
