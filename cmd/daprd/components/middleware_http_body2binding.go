//go:build allcomponents || stablecomponents

/*
Copyright 2024 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package components

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/andybalholm/brotli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	contribmiddleware "github.com/dapr/components-contrib/middleware"
	httpMiddlewareLoader "github.com/dapr/dapr/pkg/components/middleware/http"
	"github.com/dapr/dapr/pkg/middleware"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/kit/logger"
	"google.golang.org/grpc/metadata"
)

// DataMessage 定义发送到 pub/sub 的数据消息结构
type DataMessage struct {
	Timestamp    string      `json:"timestamp"`
	FunctionCode string      `json:"functionCode"`
	ActionCode   string      `json:"actionCode"`
	RequestBody  interface{} `json:"requestBody,omitempty"`
	ResponseBody interface{} `json:"responseBody,omitempty"`
	Meta         interface{} `json:"meta,omitempty"`
	Method       string      `json:"method"`
	Path         string      `json:"path"`
	Headers      interface{} `json:"headers,omitempty"`
}

type pubsubPublisher struct {
	daprGRPCPort string
	log          logger.Logger

	mu     sync.Mutex
	conn   *grpc.ClientConn
	client runtimev1pb.DaprClient
}

func newPubsubPublisher(daprGRPCPort string, log logger.Logger) *pubsubPublisher {
	return &pubsubPublisher{
		daprGRPCPort: daprGRPCPort,
		log:          log,
	}
}

func (p *pubsubPublisher) getClient() runtimev1pb.DaprClient {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client != nil {
		return p.client
	}

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%s", p.daprGRPCPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		p.log.Errorf("gRPC connect failed: %v", err)
		return nil
	}

	p.conn = conn
	p.client = runtimev1pb.NewDaprClient(conn)
	return p.client
}

// publish 通过 gRPC API 发布事件到指定的 pub/sub topic。
// 复用 gRPC 连接，避免每条消息都 NewClient。
func (p *pubsubPublisher) publish(pubsubName, topicName string, dataMsg DataMessage) {
	if pubsubName == "" {
		p.log.Debugf("publish skipped: pubsubName is empty")
		return
	}
	if topicName == "" {
		p.log.Debugf("publish skipped: topicName is empty")
		return
	}

	client := p.getClient()
	if client == nil {
		p.log.Errorf("Dapr gRPC client not available")
		return
	}

	// 获取API令牌，优先环境变量，其次metadata配置
	apiToken := os.Getenv("DAPR_API_TOKEN") // 可扩展为从metadata读取

	// 异步发送以避免阻塞请求
	go func() {
		jsonData, err := json.Marshal(dataMsg)
		if err != nil {
			p.log.Errorf("Marshal data failed: %v", err)
			return
		}
		p.log.Debugf("Publishing data to pubsub %s topic %s: %s", pubsubName, topicName, string(jsonData))

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if apiToken != "" {
			md := metadata.Pairs("dapr-api-token", apiToken)
			ctx = metadata.NewOutgoingContext(ctx, md)
		}

		_, err = client.PublishEvent(ctx, &runtimev1pb.PublishEventRequest{
			PubsubName: pubsubName,
			Topic:      topicName,
			Data:       jsonData,
		})
		if err != nil {
			p.log.Errorf("Pubsub %s publish to topic %s failed: %v", pubsubName, topicName, err)
			return
		}
		p.log.Debugf("publish OK -> pubsub=%s topic=%s", pubsubName, topicName)
	}()
}

func init() {
	httpMiddlewareLoader.DefaultRegistry.RegisterComponent(func(log logger.Logger) httpMiddlewareLoader.FactoryMethod {
		return func(metadata contribmiddleware.Metadata) (middleware.HTTP, error) {
			// 获取日志文件路径，默认为 middleware_body.log
			// logFile := metadata.Properties["logFile"]

			// 获取 pubsub 配置（必需）：默认 topic 推送
			pubsubName := metadata.Properties["pubsubName"]
			topicName := metadata.Properties["topicName"]

			// 可选：审计 topic 推送配置。
			// 当消息包含 meta（DataMessage.Meta 非空）时，将发布到 auditPubsubName+auditTopicName。
			// auditPubsubName 为空时默认复用 pubsubName。
			auditPubsubName := metadata.Properties["auditPubsubName"]
			auditTopicName := metadata.Properties["auditTopicName"]
			if auditPubsubName == "" {
				auditPubsubName = pubsubName
			}

			// 获取 Dapr gRPC 端口，优先从环境变量获取
			daprGRPCPort := os.Getenv("DAPR_GRPC_PORT")
			if daprGRPCPort == "" {
				daprGRPCPort = metadata.Properties["daprGRPCPort"]
				if daprGRPCPort == "" {
					daprGRPCPort = "3500"
				}
			}

			publisher := newPubsubPublisher(daprGRPCPort, log)

			// 获取是否记录请求体，默认为 true
			logRequest := metadata.Properties["logRequest"] != "false"

			// 获取是否记录响应体，默认为 true
			logResponse := metadata.Properties["logResponse"] != "false"

			log.Debugf("body2binding config: pubsubName=%s topicName=%s auditPubsubName=%s auditTopicName=%s daprGRPCPort=%s logRequest=%v logResponse=%v",
				pubsubName, topicName, auditPubsubName, auditTopicName, daprGRPCPort, logRequest, logResponse)

			// 获取最大记录体大小，默认为 1MB
			maxBodySize := int64(1024 * 1024) // 1MB
			if size := metadata.Properties["maxBodySize"]; size != "" {
				if parsed, err := fmt.Sscanf(size, "%d", &maxBodySize); parsed != 1 || err != nil {
					log.Warnf("Invalid maxBodySize value: %s, using default 1MB", size)
					maxBodySize = 1024 * 1024
				}
			}

			// 获取功能码和操作码的 header 字段名，默认值
			functionHeader := metadata.Properties["functionHeader"]
			if functionHeader == "" {
				functionHeader = "X-Function-Code"
			}

			actionHeader := metadata.Properties["actionHeader"]
			if actionHeader == "" {
				actionHeader = "X-Action-Code"
			}

			// 获取需要记录日志的HTTP方法，默认为 POST,PUT,DELETE
			logMethods := metadata.Properties["logMethods"]
			if logMethods == "" {
				logMethods = "POST,PUT,DELETE"
			}

			// 解析允许的HTTP方法
			allowedMethods := make(map[string]bool)
			for _, method := range strings.Split(logMethods, ",") {
				method = strings.TrimSpace(strings.ToUpper(method))
				if method != "" {
					allowedMethods[method] = true
				}
			}

			// 获取需要包含在日志中的 header keys，支持逗号分隔的列表
			includeHeaders := metadata.Properties["includeHeaders"]
			var headerKeys []string
			if includeHeaders != "" {
				for _, header := range strings.Split(includeHeaders, ",") {
					header = strings.TrimSpace(header)
					if header != "" {
						headerKeys = append(headerKeys, header)
					}
				}
			}

			// 获取路径过滤配置，支持正则表达式
			includePathsStr := metadata.Properties["includePaths"]
			excludePathsStr := metadata.Properties["excludePaths"]

			// 编译包含路径的正则表达式
			var includePathRegexes []*regexp.Regexp
			if includePathsStr != "" {
				for _, pathPattern := range strings.Split(includePathsStr, ",") {
					pathPattern = strings.TrimSpace(pathPattern)
					if pathPattern != "" {
						if regex, err := regexp.Compile(pathPattern); err != nil {
							log.Warnf("Invalid includePaths regex pattern '%s': %v", pathPattern, err)
						} else {
							includePathRegexes = append(includePathRegexes, regex)
						}
					}
				}
			}

			// 编译排除路径的正则表达式
			var excludePathRegexes []*regexp.Regexp
			if excludePathsStr != "" {
				for _, pathPattern := range strings.Split(excludePathsStr, ",") {
					pathPattern = strings.TrimSpace(pathPattern)
					if pathPattern != "" {
						if regex, err := regexp.Compile(pathPattern); err != nil {
							log.Warnf("Invalid excludePaths regex pattern '%s': %v", pathPattern, err)
						} else {
							excludePathRegexes = append(excludePathRegexes, regex)
						}
					}
				}
			}

			log.Debugf("body2binding config: functionHeader=%s actionHeader=%s allowedMethods=%v includePaths=%q excludePaths=%q includeHeaders=%v maxBodySize=%d",
				functionHeader, actionHeader, logMethods, includePathsStr, excludePathsStr, headerKeys, maxBodySize)

			return func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					// 为每个请求生成 traceID，用于串联整条决策链日志
					traceID := fmt.Sprintf("%s-%d", r.URL.Path, time.Now().UnixNano())
					log.Debugf("[%s] enter: method=%s path=%s", traceID, r.Method, r.URL.RequestURI())

					var requestBodyObj interface{}
					originalWriter := w

					// 记录请求体
					if logRequest && r.Body != nil {
						requestBody, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize))
						if err != nil {
							log.Errorf("Read request body failed: %v", err)
							requestBodyObj = map[string]string{"error": fmt.Sprintf("Read error: %v", err)}
						} else {
							requestBodyObj = decodeJSONOrString(requestBody)
							// 重置请求体供后续处理使用
							r.Body = io.NopCloser(bytes.NewReader(requestBody))
							// 打印所有请求头
							headersJson, _ := json.Marshal(r.Header)
							log.Debugf("Request headers for %s: %s", r.URL.RequestURI(), string(headersJson))
						}
					}

					var responseBodyObj interface{}
					var bodyRecorder *bodyResponseWriter
					var responseMeta interface{}

					// 如果需要记录响应体，包装响应写入器
					if logResponse {
						bodyRecorder = newBodyResponseWriter(maxBodySize)
						w = bodyRecorder
					}

					// 调用下一个处理器
					next.ServeHTTP(w, r)

					// 获取响应体并提取 meta
					if logResponse && bodyRecorder != nil {
						responseBodyBytes := bodyRecorder.body.Bytes()

						// 根据 Content-Encoding 响应头解压，支持 gzip / br(brotli) / deflate(zlib)。
						// 后端(.NET)经 nginx 压缩后可能是 brotli，仅按 gzip 魔数判断会导致解压失败、
						// 响应体被误判为 binary，进而取不到 meta。
						decodedBytes := responseBodyBytes
						contentEncoding := strings.ToLower(strings.TrimSpace(bodyRecorder.Header().Get("Content-Encoding")))
						if len(responseBodyBytes) == 0 {
							// 空响应体，无需处理
						} else if contentEncoding != "" && contentEncoding != "identity" {
							var err error
							decodedBytes, err = decompress(responseBodyBytes, contentEncoding)
							if err != nil {
								log.Warnf("path:%s, failed to decompress response body with content-encoding=%s: %v", r.URL.RequestURI(), contentEncoding, err)
								decodedBytes = responseBodyBytes
							} else {
								log.Debugf("path:%s, response body decompressed from %s", r.URL.RequestURI(), contentEncoding)
							}
						} else {
							// 没有 Content-Encoding 头时，按魔数自动探测
							if isGzipBytes(responseBodyBytes) {
								if uncompressed, err := gunzip(responseBodyBytes); err == nil {
									decodedBytes = uncompressed
									log.Debugf("path:%s, response body decompressed from gzip (magic)", r.URL.RequestURI())
								}
							} else if isBrotliBytes(responseBodyBytes) {
								if uncompressed, err := unbrotli(responseBodyBytes); err == nil {
									decodedBytes = uncompressed
									log.Debugf("path:%s, response body decompressed from brotli (magic)", r.URL.RequestURI())
								}
							}
						}

						var jsonObj interface{}
						if err := json.Unmarshal(decodedBytes, &jsonObj); err == nil {
							// 有效 JSON
							responseBodyObj = jsonObj
							log.Debugf("path:%s, response body is JSON", r.URL.RequestURI())

							// 检查是否存在 meta 字段
							if respMap, ok := jsonObj.(map[string]interface{}); ok {
								if meta, exists := respMap["meta"]; exists {
									responseMeta = meta
									// 从响应中删除 meta
									delete(respMap, "meta")
									responseBodyObj = respMap

									// 重新序列化响应体（不含 meta）并写回
									if modifiedBytes, err := json.Marshal(respMap); err == nil {
										bodyRecorder.body.Reset()
										if contentEncoding == "gzip" || isGzipBytes(responseBodyBytes) {
											// 重新 gzip 压缩后写回
											var buf bytes.Buffer
											gw := gzip.NewWriter(&buf)
											gw.Write(modifiedBytes)
											gw.Close()
											bodyRecorder.body.Write(buf.Bytes())
										} else if contentEncoding == "br" || isBrotliBytes(responseBodyBytes) {
											// 重新 brotli 压缩后写回
											var buf bytes.Buffer
											bw := brotli.NewWriter(&buf)
											bw.Write(modifiedBytes)
											bw.Close()
											bodyRecorder.body.Write(buf.Bytes())
										} else {
											bodyRecorder.body.Write(modifiedBytes)
										}
									}
								}
							}
						} else if isText(decodedBytes) {
							log.Debugf("path:%s, response body is text", r.URL.RequestURI())
							// 是文本（如UTF-8），直接保存为字符串
							responseBodyObj = string(decodedBytes)
						} else {
							log.Debugf("path:%s, response body is binary", r.URL.RequestURI())
							// 二进制，保存为 base64
							responseBodyObj = map[string]string{
								"base64": base64.StdEncoding.EncodeToString(responseBodyBytes),
							}
						}
					}

					// 记录到日志文件
					shouldLog := logRequest || logResponse
					if !shouldLog {
						log.Debugf("[%s] skip: logRequest=false && logResponse=false", traceID)
					}
					if shouldLog {
						// 检查是否为 EBR 行业类型且需要强制审计
						industryType := os.Getenv("INDUSTRY_TYPE")
						auditLog := strings.ToLower(r.Header.Get("X-Audit-Log")) == "true"
						signLog := strings.ToLower(r.Header.Get("X-Sign-Log")) == "true"
						isEBRForceAudit := industryType == "EBR" && (auditLog || signLog)
						log.Debugf("[%s] audit flags: auditLog=%v signLog=%v industryType=%s isEBRForceAudit=%v", traceID, auditLog, signLog, industryType, isEBRForceAudit)

						// 检查路径
						requestPath := r.URL.RequestURI()

						// 从 header 中获取功能码和动作码
						functionCode := r.Header.Get(functionHeader)
						actionCode := r.Header.Get(actionHeader)

						// 审计/签名日志必须携带功能码和动作码，否则不记录
						if (auditLog || signLog) && (functionCode == "" || actionCode == "") {
							log.Debugf("[%s] skip: audit/sign log missing headers functionHeader(%s)=%q actionHeader(%s)=%q", traceID, functionHeader, functionCode, actionHeader, actionCode)
							shouldLog = false
						}

						// 如果不是 EBR 强制审计模式，则执行原有的过滤逻辑
						if !isEBRForceAudit {
							// 检查当前请求方法是否在允许记录的方法列表中
							if !allowedMethods[r.Method] {
								log.Debugf("[%s] skip: method %s not in allowedMethods [%s]", traceID, r.Method, logMethods)
								shouldLog = false
							}

							// 检查路径是否应该被记录
							pathShouldLog := true

							// 如果配置了包含路径，检查当前路径是否匹配任何包含模式
							if len(includePathRegexes) > 0 {
								pathShouldLog = false
								for _, regex := range includePathRegexes {
									if regex.MatchString(requestPath) {
										pathShouldLog = true
										break
									}
								}
								if !pathShouldLog {
									log.Debugf("[%s] skip: path %s not matched by includePaths [%s]", traceID, requestPath, includePathsStr)
								}
							}

							// 如果路径通过包含检查，再检查是否在排除列表中
							if pathShouldLog && len(excludePathRegexes) > 0 {
								for _, regex := range excludePathRegexes {
									if regex.MatchString(requestPath) {
										log.Debugf("[%s] skip: path %s matched excludePaths [%s]", traceID, requestPath, excludePathsStr)
										pathShouldLog = false
										break
									}
								}
							}

							// 合并路径过滤结果
							shouldLog = shouldLog && pathShouldLog

							// 如果功能码或动作码为空，则不记录日志
							if shouldLog && (functionCode == "" || actionCode == "") {
								log.Debugf("[%s] skip: missing headers functionHeader(%s)=%q actionHeader(%s)=%q", traceID, functionHeader, functionCode, actionHeader, actionCode)
								shouldLog = false
							}
						}

						if shouldLog {
							// pub/sub 只发布模式：需要默认 pubsubName/topicName，否则不发布
							if pubsubName == "" {
								log.Debugf("[%s] skip: pubsubName not configured", traceID)
								shouldLog = false
							} else if topicName == "" {
								log.Debugf("[%s] skip: topicName not configured", traceID)
								shouldLog = false
							}
						}

						if shouldLog {
							// 获取当前时间戳
							timestamp := time.Now().Format(time.RFC3339)

							// 收集指定的 headers
							var headers interface{}
							if len(headerKeys) > 0 {
								headersMap := make(map[string]interface{})
								for _, headerKey := range headerKeys {
									if headerValues := r.Header.Values(headerKey); len(headerValues) > 0 {
										if len(headerValues) == 1 {
											// 单个值：尝试解析为 JSON，失败则作为字符串
											headersMap[headerKey] = decodeJSONOrString([]byte(headerValues[0]))
										} else {
											// 多个值：创建数组，每个值尝试解析为 JSON
											var valueArray []interface{}
											for _, value := range headerValues {
												valueArray = append(valueArray, decodeJSONOrString([]byte(value)))
											}
											headersMap[headerKey] = valueArray
										}
									}
								}
								if len(headersMap) > 0 {
									headers = headersMap
								}
							}

							// 创建数据消息结构
							dataMsg := DataMessage{
								Timestamp:    timestamp,
								FunctionCode: functionCode,
								ActionCode:   actionCode,
								Method:       r.Method,
								Path:         requestPath,
								Headers:      headers,
							}

							// 根据配置设置请求体和响应体
							if logRequest {
								dataMsg.RequestBody = requestBodyObj
							}
							if logResponse {
								// 设置响应体（不含 meta 的完整响应）
								dataMsg.ResponseBody = responseBodyObj
							}

							// 检查是否需要记录 meta（复用前面的变量）
							includeMeta := auditLog || signLog

							// 如果需要记录 meta 且 meta 存在，将 meta 单独记录到 Meta 字段
							if includeMeta && responseMeta != nil {
								dataMsg.Meta = responseMeta
							} else if responseMeta != nil {
								log.Debugf("[%s] response has meta but auditLog=%v signLog=%v, meta dropped", traceID, auditLog, signLog)
							}

							log.Debugf("[%s] logging: method=%s path=%s functionCode=%s actionCode=%s hasMeta=%v", traceID, dataMsg.Method, dataMsg.Path, dataMsg.FunctionCode, dataMsg.ActionCode, dataMsg.Meta != nil)

							// 推送：带审计/签名标记(X-Audit-Log / X-Sign-Log)的消息发到审计 topic；其他只发默认 topic。
							// 审计 topic 的触发以请求头为准，而不是依赖响应中的 meta 字段。
							if (auditLog || signLog) && auditPubsubName != "" && auditTopicName != "" {
								log.Debugf("[%s] publish to audit topic: pubsub=%s topic=%s", traceID, auditPubsubName, auditTopicName)
								publisher.publish(auditPubsubName, auditTopicName, dataMsg)
							}
							// 发到默认 topic
							log.Debugf("[%s] publish to default topic: pubsub=%s topic=%s", traceID, pubsubName, topicName)
							publisher.publish(pubsubName, topicName, dataMsg)
						}
					}

					// 将（可能已删除 meta 的）最终响应写回给调用方
					if logResponse && bodyRecorder != nil {
						if err := bodyRecorder.WriteTo(originalWriter); err != nil {
							log.Errorf("Write response back failed: %v", err)
						}
					}
				})
			}, nil
		}
	}, "body2binding")
}

// isGzipBytes 判断是否为 gzip 压缩数据（魔数 0x1f 0x8b）。
func isGzipBytes(data []byte) bool {
	return len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b
}

// isBrotliBytes 判断是否可能是 brotli 压缩数据。
// brotli 没有统一魔数，这里按常见前几个字节特征做保守判断：首字节为 0x0b 或 0x1b 等窗口标记。
func isBrotliBytes(data []byte) bool {
	if len(data) < 2 {
		return false
	}
	// brotli 流的第一个字节通常是 0x0b（window 16）或 0x1b 等，第二个字节通常是 0x06 或 0x2e 之类
	return data[0] == 0x0b || data[0] == 0x1b || data[0] == 0x81
}

// gunzip 解压 gzip 数据。
func gunzip(data []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}

// unbrotli 解压 brotli 数据。
func unbrotli(data []byte) ([]byte, error) {
	br := brotli.NewReader(bytes.NewReader(data))
	return io.ReadAll(br)
}

// decompress 根据 content-encoding 解压响应体，支持 gzip、br(brotli)、deflate(zlib)。
func decompress(data []byte, contentEncoding string) ([]byte, error) {
	enc := strings.TrimSpace(strings.ToLower(contentEncoding))
	switch {
	case enc == "gzip" || enc == "x-gzip":
		return gunzip(data)
	case enc == "br" || enc == "brotli":
		return unbrotli(data)
	case enc == "deflate":
		zr, err := zlib.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return io.ReadAll(zr)
	default:
		return data, nil
	}
}

// 判断是否为文本内容（简单判断，ASCII范围）
func isText(data []byte) bool {
	// 允许常见的控制字符（如换行、回车、制表符）
	for _, b := range data {
		if (b < 0x09 || (b > 0x0D && b < 0x20) || b > 0x7E) && b != 0x0A && b != 0x0D {
			return false
		}
	}
	return true
}

func decodeJSONOrString(data []byte) interface{} {
	var jsonObj interface{}
	if err := json.Unmarshal(data, &jsonObj); err != nil {
		return string(data)
	}
	return jsonObj
}

// bodyResponseWriter 包装 http.ResponseWriter 以捕获响应体
type bodyResponseWriter struct {
	header  http.Header
	body    *bytes.Buffer
	maxSize int64
	status  int
}

func newBodyResponseWriter(maxSize int64) *bodyResponseWriter {
	return &bodyResponseWriter{
		header:  make(http.Header),
		body:    &bytes.Buffer{},
		maxSize: maxSize,
	}
}

func (brw *bodyResponseWriter) Write(p []byte) (int, error) {
	// 如果缓冲区大小未超过限制，则记录响应体
	if int64(brw.body.Len()+len(p)) <= brw.maxSize {
		brw.body.Write(p)
	} else if brw.body.Len() < int(brw.maxSize) {
		// 如果当前缓冲区未满但添加新数据会超过限制，则只记录部分数据
		remaining := brw.maxSize - int64(brw.body.Len())
		brw.body.Write(p[:remaining])
	}

	// 不直接写入原始响应，等 next 结束后统一处理/修改再写回
	return len(p), nil
}

// Header 返回响应头
func (brw *bodyResponseWriter) Header() http.Header {
	return brw.header
}

// WriteHeader 写入响应状态码
func (brw *bodyResponseWriter) WriteHeader(statusCode int) {
	if brw.status == 0 {
		brw.status = statusCode
	}
}

func (brw *bodyResponseWriter) Flush() {}

func (brw *bodyResponseWriter) WriteTo(w http.ResponseWriter) error {
	// 复制 headers（避免共享底层 slice）
	for k, vv := range brw.header {
		copied := make([]string, len(vv))
		copy(copied, vv)
		w.Header()[k] = copied
	}
	// body 可能被修改，删除 Content-Length 让 net/http 自动计算
	w.Header().Del("Content-Length")

	statusCode := brw.status
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	w.WriteHeader(statusCode)
	_, err := w.Write(brw.body.Bytes())
	return err
}
