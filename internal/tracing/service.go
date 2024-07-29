// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package tracing

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"pubsub-horizon-golaris/internal/config"
	"strings"
)

func Initialize() {
	var exporter = createJaegerExporter(config.Current.Tracing.CollectorEndpoint)
	if exporter != nil {
		var provider = tracesdk.NewTracerProvider(
			tracesdk.WithBatcher(exporter),
			tracesdk.WithResource(
				resource.NewWithAttributes(
					semconv.SchemaURL,
					semconv.ServiceNameKey.String("horizon"))))

		otel.SetTracerProvider(provider)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			b3.New(b3.WithInjectEncoding(b3.B3MultipleHeader)),
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	}
}

func createJaegerExporter(url string) tracesdk.SpanExporter {
	var options = []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(config.Current.Tracing.CollectorEndpoint),
	}

	if !strings.HasPrefix(config.Current.Tracing.CollectorEndpoint, "https") {
		options = append(options, otlptracehttp.WithInsecure())
	}

	exporter, err := otlptrace.New(context.Background(), otlptracehttp.NewClient(options...))
	if err != nil {
		log.Error().Err(err).Msg("Could not create trace exporter")
		return nil
	}

	return exporter
}
