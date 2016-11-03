package http

import (
	"log"
	_ "net/http/pprof"

	"github.com/gin-gonic/gin"
	"falcon-index/g"
)

var router *gin.Engine

func init() {
	router = gin.Default()
	//gin.SetMode(gin.ReleaseMode)
	configApiQueryRoutes()
	configApiInsertRoutes()
}

func Start() {
	if !g.Config().Http.Enabled {
		log.Println("http.Start warning, not enabled")
		return
	}

	addr := g.Config().Http.Listen
	if addr == "" {
		return
	}

	router.Run(addr)
}
