package service

import "github.com/gofiber/fiber/v2"

func getCircuitBreakerMessage(c *fiber.Ctx) error {
	return c.SendString("Hello, World!")
}
