package notify

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"time"

	"eni.telekom.de/galileo/client/galileo"
	"eni.telekom.de/galileo/client/options"
	"github.com/rs/zerolog/log"
)

var CurrentSender *NotificationSender

const (
	resolver = "TEAMS"
)

type RetryConfig struct {
	MaxRetries  int           // MaxRetries is the maximum number of retry attempts.
	Cooldown    time.Duration // Cooldown is the initial delay before the first attempt.
	BackoffBase float64       // BackoffBase is the base multiplier for the exponential backoff calculation.
	MaxBackoff  time.Duration // MaxBackoff is the maximum allowed backoff duration.
}

// NotificationSender encapsulates the Galileo client and logic for sending notifications.
type NotificationSender struct {
	client      *galileo.Client
	retryConfig RetryConfig
	rand        *rand.Rand
}

func newNotificationSender(clientOpts *options.ClientOptions) *NotificationSender {
	client := galileo.NewClient(clientOpts)

	defaultRetryConfig := RetryConfig{
		MaxRetries:  5,
		Cooldown:    2 * time.Second,
		BackoffBase: 2.0,
		MaxBackoff:  1 * time.Minute,
	}

	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	return &NotificationSender{
		client:      client,
		retryConfig: defaultRetryConfig,
		rand:        seededRand,
	}
}

func (h *NotificationSender) SetRetryConfig(cfg RetryConfig) {
	h.retryConfig = cfg
}

func wait(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SendNotification sends a notifications based on the provided payload.
//
// Parameters:
// - ctx: The context for managing request deadlines and cancellations.
// - opts: The options applied to the notification request that is being sent to Galileo.
//
// Returns:
// - An error if the notification could not be sent after all retries; nil otherwise.
// Note: It will retry sending the notification upon failure using exponential backoff with jitter.
// Jitter is used to avoid the “Thundering Herd” effect when calling the Notification Service,
// which could occur in certain operating states.
func (h *NotificationSender) SendNotification(ctx context.Context, opts *options.NotifyOptions) error {
	log.Debug().Msg("Starting SendNotification process")
	cfg := h.retryConfig

	// Initial cooldown (context-sensitive)
	if cfg.Cooldown > 0 {
		log.Debug().Dur("cooldown", cfg.Cooldown).Msg("Applying initial cooldown")
		if err := wait(ctx, cfg.Cooldown); err != nil {
			return fmt.Errorf("initial cooldown aborted: %w", err)
		}
	}

	// Retry loop: total of MaxRetries+1 attempts.
	for i := 0; i <= cfg.MaxRetries; i++ {
		log.Debug().Int("attempt", i+1).Msg("Attempting to send notification")
		err := h.sendNotificationRequest(opts)
		if err == nil {
			log.Debug().Msg("Notification sent successfully")
			return nil
		}

		// Immediately abort if the context has been canceled.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		log.Warn().Err(err).Int("attempt", i+1).Msg("Notification send attempt failed")

		/// If the last attempt failed, return an error.
		if i == cfg.MaxRetries {
			log.Debug().Int("maxRetries", cfg.MaxRetries).Msg("Failed to send notification after all retry attempts")
			return fmt.Errorf("failed to send notification after %d attempts: %w", cfg.MaxRetries, err)
		}

		// Calculate the backoff duration including jitter.
		backoffDuration := h.calculateBackoff(i, cfg.BackoffBase, cfg.MaxBackoff)
		log.Debug().Dur("backoff", backoffDuration).Int("attempt", i+1).Msg("Waiting before next attempt")

		// Wait for the backoff duration, but respect context cancellation.
		if err := wait(ctx, backoffDuration); err != nil {
			return fmt.Errorf("waiting aborted: %w", err)
		}
	}

	log.Error().Msg("Unexpected error in retry loop")
	return errors.New("unexpected error in retry loop")
}

// calculateBackoff calculates the wait duration for the next retry attempt using exponential backoff with jitter.
//
// Parameters:
// - attempt: The current retry attempt (starting from 0).
// - backoffBase: The base multiplier for calculating exponential backoff.
// - maxBackoff: The maximum allowed backoff duration.
//
// Returns:
// - A time.Duration representing the calculated backoff time with jitter applied.
//
// Note:
// - The jitter is applied as a random percentage between -25% and +25% of the base backoff duration.
func (h *NotificationSender) calculateBackoff(attempt int, backoffBase float64, maxBackoff time.Duration) time.Duration {
	log.Debug().
		Int("attempt", attempt).
		Float64("backoffBase", backoffBase).
		Dur("maxBackoff", maxBackoff).
		Msg("Calculating backoff")

	// Base backoff: backoffBase^attempt seconds.
	baseDelay := time.Duration(math.Pow(backoffBase, float64(attempt))) * time.Second
	if baseDelay > maxBackoff {
		log.Debug().Dur("backoff", baseDelay).Dur("maxBackoff", maxBackoff).Msg("Backoff exceeded maxBackoff, capping")
		baseDelay = maxBackoff
	}

	// Calculate jitter: random percentage between -25% and +25% of the base delay.
	jitterPercent := h.rand.Float64()*0.5 - 0.25
	jitter := time.Duration(float64(baseDelay) * jitterPercent)

	finalBackoff := baseDelay + jitter
	if finalBackoff < 0 {
		log.Warn().Dur("backoff", finalBackoff).Msg("Negative backoff calculated, setting to 0")
		finalBackoff = 0
	}

	log.Debug().Dur("finalBackoff", finalBackoff).Msg("Final backoff duration calculated")
	return finalBackoff
}

// sendNotificationRequest sends a single notifications request to the Galileo client.
//
// Parameters:
// - ctx: The context for managing request deadlines and cancellations.
// - opts: The options applied to the notification request that is being sent to Galileo.
//
// Returns:
// - An error if the request failed; nil otherwise.
func (h *NotificationSender) sendNotificationRequest(opts *options.NotifyOptions) error {
	log.Debug().Msg("Sending notification request to Notification Service")
	res, err := h.client.Notify(resolver, opts)
	if err != nil {
		log.Error().Err(err).Msg("Error sending notification request")
		return fmt.Errorf("error sending notification: %w", err)
	}

	log.Info().Str("response", res.Message).Msg("Notification sent successfully")
	return nil
}

func Initialize() {
	var handler *NotificationSender
	if cfg := config.Current.Notifications; cfg.Enabled {
		handler = newNotificationSender(cfg.Options())
		log.Info().Msg("Notification handler initialized")

		circuitBreakerCache := config.Current.Hazelcast.Caches.CircuitBreakerCache
		if err := cache.CircuitBreakerCache.AddListener(circuitBreakerCache, new(NotificationListener)); err != nil {
			log.Error().Err(err).Msg("Failed to add circuit breaker cache listener for sending notifications")
		}
	}

	CurrentSender = handler
}
