// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package opa

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dapr/components-contrib/middleware"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/docker/distribution/context"
	"github.com/open-policy-agent/opa/rego"
	"github.com/valyala/fasthttp"
)

type opaMiddlewareMetadata struct {
	ConfigPath string `json:"configPath"`
	regoPath   string `json:"regoPath"`
}

// NewOPAMiddleware returns a new OPA middleware
func NewOPAMiddleware(logger logger.Logger) *Middleware {
	return &Middleware{logger: logger}
}

// Middleware is an OPA authorization middleware
type Middleware struct {
	logger logger.Logger
}

// GetHandler returns the HTTP handler provided by the middleware
func (m *Middleware) GetHandler(metadata middleware.Metadata) (func(h fasthttp.RequestHandler) fasthttp.RequestHandler, error) {
	meta, err := m.getNativeMetadata(metadata)
	if err != nil {
		return nil, err
	}

	var opts []func(*OPA) error
	if meta.ConfigPath != "" {
		opts = append(opts, Config(meta.ConfigPath))
	}
	engine, err := New(opts...)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	if err = engine.Start(ctx); err != nil {
		return nil, err
	}

	return func(h fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			// TODO: expand to capture attributes
			path := strings.Split(strings.Trim(string(ctx.Path()), "/"), "/")
			input := map[string]interface{}{
				"method": string(ctx.Method()),
				"path":   path,
			}

			var ropts []func(r *rego.Rego)
			if meta.regoPath != "" {
				ropts = append(ropts, rego.Load([]string{meta.regoPath}, nil))
			}

			allowed, err := engine.Check(ctx, input, ropts...)
			if err != nil {
				ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
			} else if !allowed {
				ctx.Error(err.Error(), fasthttp.StatusForbidden)
			} else {
				h(ctx)
			}
		}
	}, nil
}

func (m *Middleware) getNativeMetadata(metadata middleware.Metadata) (*opaMiddlewareMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var middlewareMetadata opaMiddlewareMetadata
	err = json.Unmarshal(b, &middlewareMetadata)
	if err != nil {
		return nil, err
	}

	if middlewareMetadata.ConfigPath == "" && middlewareMetadata.regoPath == "" {
		return nil, fmt.Errorf("either `regoFile` or `ConfigPath` metadata property required")
	}

	return &middlewareMetadata, nil
}
