package http

import (
	_ "fmt"
	"github.com/gin-gonic/gin"
	"falcon-index/index"
	_ "strconv"
	_ "strings"
)

func configApiInsertRoutes() {
	router.GET("/hello", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "zhaoyoushuai",
		})
	})

	router.GET("/insert", func(c *gin.Context){
		q := c.DefaultQuery("q", "")
		err := index.HttpBuildIndex(q)
		err_s := err.Error()
		if err_s != "no error" {
			c.JSON(500, gin.H{
				"message": err_s,
			})
		}else{
			c.JSON(200, gin.H{
				"message": "insert success",
			})
		}
	})
}