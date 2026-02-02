package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
)

type APIResponse struct {
	Data interface{} `json:"data"`
	Meta []string    `json:"meta"`
}

type User struct {
	ID      int       `json:"id"`
	Name    string    `json:"name"`
	Email   string    `json:"email"`
	Created time.Time `json:"created"`
}

type CreateUserRequest struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

var users []User
var nextID = 1

func main() {
	r := mux.NewRouter()

	// API 路由
	r.HandleFunc("/api/users", createUser).Methods("POST")
	r.HandleFunc("/api/users", getUsers).Methods("GET")
	r.HandleFunc("/api/users/{id}", getUser).Methods("GET")
	r.HandleFunc("/health", healthCheck).Methods("GET")

	// 启动服务器
	port := os.Getenv("APP_PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Printf("服务器启动在端口 %s\n", port)
	fmt.Println("API 端点:")
	fmt.Println("  POST /api/users - 创建用户")
	fmt.Println("  GET /api/users - 获取所有用户")
	fmt.Println("  GET /api/users/{id} - 获取指定用户")
	fmt.Println("  GET /health - 健康检查")
	fmt.Println("\n注意：确保 Dapr sidecar 已启动并配置了 body 中间件")

	log.Fatal(http.ListenAndServe(":"+port, r))
}

func createUser(w http.ResponseWriter, r *http.Request) {
	// 从 header 中获取模块码和动作码（用于演示）
	moduleCode := r.Header.Get("X-Module-Code")
	actionCode := r.Header.Get("X-Action-Code")

	log.Printf("处理请求：模块码=%s, 动作码=%s", moduleCode, actionCode)

	var req CreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "无效的请求体", http.StatusBadRequest)
		return
	}

	if req.Name == "" || req.Email == "" {
		http.Error(w, "姓名和邮箱不能为空", http.StatusBadRequest)
		return
	}

	user := User{
		ID:      nextID,
		Name:    req.Name,
		Email:   req.Email,
		Created: time.Now(),
	}
	users = append(users, user)
	nextID++

	resp := APIResponse{
		Data: user,
		Meta: []string{"createUser", "test-meta"},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func getUsers(w http.ResponseWriter, r *http.Request) {
	resp := APIResponse{
		Data: users,
		Meta: []string{"getUsers", "test-meta"},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func getUser(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	for _, user := range users {
		if fmt.Sprintf("%d", user.ID) == id {
			resp := APIResponse{
				Data: user,
				Meta: []string{"getUser", "test-meta"},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}
	}
	http.Error(w, "用户不存在", http.StatusNotFound)
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	resp := APIResponse{
		Data: map[string]interface{}{
			"status":    "healthy",
			"timestamp": time.Now(),
			"users":     len(users),
		},
		Meta: []string{"healthCheck", "test-meta"},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
