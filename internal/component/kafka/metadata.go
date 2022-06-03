/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kafka

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

const (
	key                  = "partitionKey"
	skipVerify           = "skipVerify"
	caCert               = "caCert"
	clientCert           = "clientCert"
	clientKey            = "clientKey"
	consumeRetryEnabled  = "consumeRetryEnabled"
	consumeRetryInterval = "consumeRetryInterval"
	authType             = "authType"
	passwordAuthType     = "password"
	oidcAuthType         = "oidc"
	mtlsAuthType         = "mtls"
	noAuthType           = "none"
)

type KafkaMetadata struct {
	Brokers              []string            `jsonschema:"title=brokers,description=The list of brokers, example=localhost:9092,required"`
	ConsumerGroup        string              `jsonschema:"title=consumer group,description=The consumer group id, example=my-consumer-group,required"`
	ClientID             string              `jsonschema:"title=client id,description=The client id, example=my-client-id,required"`
	AuthType             string              `jsonschema:"title=auth type,description=The auth type, example=none,required"`
	SaslUsername         string              `jsonschema:"title=sasl username,description=The sasl username, example=my-sasl-username,required"`
	SaslPassword         string              `jsonschema:"title=sasl password,description=The sasl password, example=my-sasl-password,required"`
	InitialOffset        int64               `jsonschema:"title=initial offset,description=The initial offset, example=0,required"`
	MaxMessageBytes      int                 `jsonschema:"title=max message bytes,description=The max message bytes, example=1048576,required"`
	OidcTokenEndpoint    string              `jsonschema:"title=oidc token endpoint,description=The oidc token endpoint, example=https://oidc.example.com/token,required"`
	OidcClientID         string              `jsonschema:"title=oidc client id,description=The oidc client id, example=my-oidc-client-id,required"`
	OidcClientSecret     string              `jsonschema:"title=oidc client secret,description=The oidc client secret, example=my-oidc-client-secret,required"`
	OidcScopes           []string            `jsonschema:"title=oidc scopes,description=The oidc scopes, example=openid profile email,required"`
	TLSDisable           bool                `jsonschema:"title=tls disable,description=The tls disable, example=false,required"`
	TLSSkipVerify        bool                `jsonschema:"title=tls skip verify,description=The tls skip verify, example=false,required"`
	TLSCaCert            string              `jsonschema:"title=tls ca cert,description=The tls ca cert, example=my-ca-cert,required"`
	TLSClientCert        string              `jsonschema:"title=tls client cert,description=The tls client cert, example=my-client-cert,required"`
	TLSClientKey         string              `jsonschema:"title=tls client key,description=The tls client key, example=my-client-key,required"`
	ConsumeRetryEnabled  bool                `jsonschema:"title=consume retry enabled,description=The consume retry enabled, example=false,required"`
	ConsumeRetryInterval time.Duration       `jsonschema:"title=consume retry interval,description=The consume retry interval, example=1s,required"`
	Version              sarama.KafkaVersion `jsonschema:"title=version,description=The version, example=1.0.0,required"`
}

// upgradeMetadata updates metadata properties based on deprecated usage.
func (k *Kafka) upgradeMetadata(metadata map[string]string) (map[string]string, error) {
	authTypeVal, authTypePres := metadata[authType]
	authReqVal, authReqPres := metadata["authRequired"]
	saslPassVal, saslPassPres := metadata["saslPassword"]

	// If authType is not set, derive it from authRequired.
	if (!authTypePres || authTypeVal == "") && authReqPres && authReqVal != "" {
		k.logger.Warn("AuthRequired is deprecated, use AuthType instead.")
		validAuthRequired, err := strconv.ParseBool(authReqVal)
		if err == nil {
			if validAuthRequired {
				// If legacy authRequired was used, either SASL username or mtls is the method.
				if saslPassPres && saslPassVal != "" {
					// User has specified saslPassword, so intend for password auth.
					metadata[authType] = passwordAuthType
				} else {
					metadata[authType] = mtlsAuthType
				}
			} else {
				metadata[authType] = noAuthType
			}
		} else {
			return metadata, errors.New("kafka error: invalid value for 'authRequired' attribute")
		}
	}

	// if consumeRetryEnabled is not present, use component default value
	consumeRetryEnabledVal, consumeRetryEnabledPres := metadata[consumeRetryEnabled]
	if !consumeRetryEnabledPres || consumeRetryEnabledVal == "" {
		metadata[consumeRetryEnabled] = strconv.FormatBool(k.DefaultConsumeRetryEnabled)
	}

	return metadata, nil
}

// getKafkaMetadata returns new Kafka metadata.
func (k *Kafka) getKafkaMetadata(metadata map[string]string) (*KafkaMetadata, error) {
	meta := KafkaMetadata{
		ConsumeRetryInterval: 100 * time.Millisecond,
	}
	// use the runtimeConfig.ID as the consumer group so that each dapr runtime creates its own consumergroup
	if val, ok := metadata["consumerID"]; ok && val != "" {
		meta.ConsumerGroup = val
		k.logger.Debugf("Using %s as ConsumerGroup", meta.ConsumerGroup)
		k.logger.Warn("ConsumerID is deprecated, if ConsumerID and ConsumerGroup are both set, ConsumerGroup is used")
	}

	if val, ok := metadata["consumerGroup"]; ok && val != "" {
		meta.ConsumerGroup = val
		k.logger.Debugf("Using %s as ConsumerGroup", meta.ConsumerGroup)
	}

	if val, ok := metadata["clientID"]; ok && val != "" {
		meta.ClientID = val
		k.logger.Debugf("Using %s as ClientID", meta.ClientID)
	}

	initialOffset, err := parseInitialOffset(metadata["initialOffset"])
	if err != nil {
		return nil, err
	}
	meta.InitialOffset = initialOffset

	if val, ok := metadata["brokers"]; ok && val != "" {
		meta.Brokers = strings.Split(val, ",")
	} else {
		return nil, errors.New("kafka error: missing 'brokers' attribute")
	}

	k.logger.Debugf("Found brokers: %v", meta.Brokers)

	val, ok := metadata["authType"]
	if !ok {
		return nil, errors.New("kafka error: missing 'authType' attribute")
	}
	if val == "" {
		return nil, errors.New("kafka error: 'authType' attribute was empty")
	}

	switch strings.ToLower(val) {
	case passwordAuthType:
		meta.AuthType = val
		if val, ok = metadata["saslUsername"]; ok && val != "" {
			meta.SaslUsername = val
		} else {
			return nil, errors.New("kafka error: missing SASL Username for authType 'password'")
		}

		if val, ok = metadata["saslPassword"]; ok && val != "" {
			meta.SaslPassword = val
		} else {
			return nil, errors.New("kafka error: missing SASL Password for authType 'password'")
		}

		k.logger.Debug("Configuring SASL password authentication.")
	case oidcAuthType:
		meta.AuthType = val
		if val, ok = metadata["oidcTokenEndpoint"]; ok && val != "" {
			meta.OidcTokenEndpoint = val
		} else {
			return nil, errors.New("kafka error: missing OIDC Token Endpoint for authType 'oidc'")
		}
		if val, ok = metadata["oidcClientID"]; ok && val != "" {
			meta.OidcClientID = val
		} else {
			return nil, errors.New("kafka error: missing OIDC Client ID for authType 'oidc'")
		}
		if val, ok = metadata["oidcClientSecret"]; ok && val != "" {
			meta.OidcClientSecret = val
		} else {
			return nil, errors.New("kafka error: missing OIDC Client Secret for authType 'oidc'")
		}
		if val, ok = metadata["oidcScopes"]; ok && val != "" {
			meta.OidcScopes = strings.Split(val, ",")
		} else {
			k.logger.Warn("Warning: no OIDC scopes specified, using default 'openid' scope only. This is a security risk for token reuse.")
			meta.OidcScopes = []string{"openid"}
		}
		k.logger.Debug("Configuring SASL token authentication via OIDC.")
	case mtlsAuthType:
		meta.AuthType = val
		if val, ok = metadata[clientCert]; ok && val != "" {
			if !isValidPEM(val) {
				return nil, errors.New("kafka error: invalid client certificate")
			}
			meta.TLSClientCert = val
		}
		if val, ok = metadata[clientKey]; ok && val != "" {
			if !isValidPEM(val) {
				return nil, errors.New("kafka error: invalid client key")
			}
			meta.TLSClientKey = val
		}
		// clientKey and clientCert need to be all specified or all not specified.
		if (meta.TLSClientKey == "") != (meta.TLSClientCert == "") {
			return nil, errors.New("kafka error: clientKey or clientCert is missing")
		}
		k.logger.Debug("Configuring mTLS authentication.")
	case noAuthType:
		meta.AuthType = val
		k.logger.Debug("No authentication configured.")
	default:
		return nil, errors.New("kafka error: invalid value for 'authType' attribute")
	}

	if val, ok := metadata["maxMessageBytes"]; ok && val != "" {
		maxBytes, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("kafka error: cannot parse maxMessageBytes: %w", err)
		}

		meta.MaxMessageBytes = maxBytes
	}

	if val, ok := metadata[caCert]; ok && val != "" {
		if !isValidPEM(val) {
			return nil, errors.New("kafka error: invalid ca certificate")
		}
		meta.TLSCaCert = val
	}

	if val, ok := metadata["disableTls"]; ok && val != "" {
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("kafka: invalid value for 'tlsDisable' attribute: %w", err)
		}
		meta.TLSDisable = boolVal
		if meta.TLSDisable {
			k.logger.Info("kafka: TLS connectivity to broker disabled")
		}
	}

	if val, ok := metadata[skipVerify]; ok && val != "" {
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("kafka error: invalid value for '%s' attribute: %w", skipVerify, err)
		}
		meta.TLSSkipVerify = boolVal
		if boolVal {
			k.logger.Infof("kafka: you are using 'skipVerify' to skip server config verify which is unsafe!")
		}
	}

	if val, ok := metadata[consumeRetryEnabled]; ok && val != "" {
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("kafka error: invalid value for '%s' attribute: %w", consumeRetryEnabled, err)
		}
		meta.ConsumeRetryEnabled = boolVal
	}

	if val, ok := metadata[consumeRetryInterval]; ok && val != "" {
		durationVal, err := time.ParseDuration(val)
		if err != nil {
			intVal, err := strconv.ParseUint(val, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("kafka error: invalid value for '%s' attribute: %w", consumeRetryInterval, err)
			}
			durationVal = time.Duration(intVal) * time.Millisecond
		}
		meta.ConsumeRetryInterval = durationVal
	}

	if val, ok := metadata["version"]; ok && val != "" {
		version, err := sarama.ParseKafkaVersion(val)
		if err != nil {
			return nil, errors.New("kafka error: invalid kafka version")
		}
		meta.Version = version
	} else {
		meta.Version = sarama.V2_0_0_0
	}

	return &meta, nil
}
