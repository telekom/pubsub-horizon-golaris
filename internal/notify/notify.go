package notify

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"eni.telekom.de/galileo/client/galileo"
	"eni.telekom.de/galileo/client/options"
	"github.com/rs/zerolog/log"
)

const (
	resolver = "TEAMS"
)

type RetryConfig struct {
	MaxRetries  int           // MaxRetries is the maximum number of retry attempts.
	Cooldown    time.Duration // Cooldown is the initial delay before the first attempt.
	BackoffBase float64       // BackoffBase is the base multiplier for the exponential backoff calculation.
	MaxBackoff  time.Duration // MaxBackoff is the maximum allowed backoff duration.
}

// NotificationHandler encapsulates the Galileo client and logic for sending notifications.
type NotificationHandler struct {
	client      *galileo.Client
	retryConfig RetryConfig
	rand        *rand.Rand
}

// NewNotificationHandler creates a new NotificationHandler with a configured Galileo client.
// Parameters:
// - baseURL: The base URL of the Galileo service.
// - issuer: The token issuer URL for authentication.
// - clientID: The client ID for OAuth2 authentication.
// - clientSecret: The client secret for OAuth2 authentication.
// Returns:
// - A pointer to an initialized NotificationHandler.
func NewNotificationHandler(baseURL, issuer, clientID, clientSecret string) *NotificationHandler {
	log.Debug().Msg("Initializing new NotificationHandler")

	clientOpts := options.Client().
		SetDebug(false).
		SetDryRun(false).
		SetBaseUrl(baseURL).
		SetTimeout(30 * time.Second).
		WithAuth(
			options.Auth().
				SetEnabled(true).
				SetIssuer(issuer).
				SetClientId(clientID).
				SetClientSecret(clientSecret),
		)
	client := galileo.NewClient(clientOpts)
	log.Debug().Msg("NotificationHandler initialized")

	// Set default values for the retry configuration.
	defaultRetryConfig := RetryConfig{
		MaxRetries:  5,
		Cooldown:    2 * time.Second,
		BackoffBase: 2.0,
		MaxBackoff:  1 * time.Minute,
	}

	// Create a new random generator seeded with the current time.
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	return &NotificationHandler{
		client:      client,
		retryConfig: defaultRetryConfig,
		rand:        seededRand,
	}
}

// SetRetryConfig allows adjusting the retry parameters.
func (h *NotificationHandler) SetRetryConfig(cfg RetryConfig) {
	h.retryConfig = cfg
}

// wait is a utility function which pauses execution for the specified duration or returns earlier if the context is canceled.
// This function is used to avoid an implementation with direct time.Sleep calls and reacts to ctx.Done().
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
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
func (h *NotificationHandler) SendNotification(ctx context.Context, opts *options.NotifyOptions) error {
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
func (h *NotificationHandler) calculateBackoff(attempt int, backoffBase float64, maxBackoff time.Duration) time.Duration {
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
func (h *NotificationHandler) sendNotificationRequest(opts *options.NotifyOptions) error {
	log.Debug().Interface("opts", opts).Msg("Preparing to send notification request")

	notifyOpts := options.Notify().
		SetParameters(opts.Parameters).
		SetSender(opts.Sender).
		SetTemplate(opts.Template).
		SetSubject(opts.Subject).
		SetData(opts.Data)

	log.Debug().Msg("Sending notification request to Notification Service")
	res, err := h.client.Notify(resolver, notifyOpts)
	if err != nil {
		log.Error().Err(err).Msg("Error sending notification request")
		return fmt.Errorf("error sending notification: %w", err)
	}

	log.Info().Str("response", res.Message).Msg("Notification sent successfully")
	return nil
}
