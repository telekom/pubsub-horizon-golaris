// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package tracing

import (
	"context"
	"github.com/gofiber/contrib/otelfiber/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"pubsub-horizon-golaris/internal/config"
)

// ToDo: Maybe remove the tracer to horizon2Go
func initTracer() {
	exporterOpts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(config.Current.Tracing.CollectorEndpoint),
	}

	if !config.Current.Tracing.Https {
		exporterOpts = append(exporterOpts, otlptracehttp.WithInsecure())
	}

	exporter, err := otlptrace.New(context.Background(), otlptracehttp.NewClient(exporterOpts...))
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create OTLP exporter")
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("horizon"))))

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(b3.New(b3.WithInjectEncoding(b3.B3MultipleHeader)))
}

func Middleware() fiber.Handler {
	if config.Current.Tracing.Enabled {
		initTracer()
		return otelfiber.Middleware(otelfiber.WithCollectClientIP(false))
	} else {
		return func(ctx *fiber.Ctx) error {
			return ctx.Next()
		}
	}
}
