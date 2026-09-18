package otelx

import (
	"fmt"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

// EchoMiddleware درخواست‌های HTTP را با span و context propagation پوشش می‌دهد.
func EchoMiddleware(service string) echo.MiddlewareFunc {
	if service == "" {
		service = "arvan-api"
	}
	tracer := otel.Tracer(service)
	propagator := otel.GetTextMapPropagator()

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			req := c.Request()
			ctx := propagator.Extract(req.Context(), propagation.HeaderCarrier(req.Header))

			spanName := fmt.Sprintf("%s %s", req.Method, c.Path())
			if c.Path() == "" {
				spanName = req.Method + " " + req.URL.Path
			}

			ctx, span := tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					semconv.HTTPRequestMethodKey.String(req.Method),
					semconv.URLPath(req.URL.Path),
					attribute.String("http.route", c.Path()),
				),
			)
			defer span.End()

			c.SetRequest(req.WithContext(ctx))
			err := next(c)

			status := c.Response().Status
			span.SetAttributes(semconv.HTTPResponseStatusCode(status))
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			} else if status >= 500 {
				span.SetStatus(codes.Error, fmt.Sprintf("http_%d", status))
			}
			return err
		}
	}
}
