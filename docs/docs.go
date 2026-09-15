package docs

import (
	_ "embed"
	"net/http"

	"github.com/flowchartsman/swaggerui"
	"github.com/labstack/echo/v4"
)

//go:embed swagger.yaml
var openAPI []byte

func Register(e *echo.Echo) {
	e.GET("/openapi.yaml", func(c echo.Context) error {
		return c.Blob(http.StatusOK, "application/yaml", openAPI)
	})

	e.GET("/swagger", func(c echo.Context) error {
		return c.Redirect(http.StatusMovedPermanently, "/swagger/")
	})

	e.GET("/swagger/*", echo.WrapHandler(http.StripPrefix("/swagger", swaggerui.Handler(openAPI))))
}
