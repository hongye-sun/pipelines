package server

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	api "github.com/kubeflow/pipelines/backend/api/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/resource"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
)

func TestCreatePipeline_YAML(t *testing.T) {
	httpServer := getMockServer(t)
	// Close the server when test finishes
	defer httpServer.Close()

	clientManager := resource.NewFakeClientManagerOrFatal(util.NewFakeTimeForEpoch())
	resourceManager := resource.NewResourceManager(clientManager)

	pipelineServer := PipelineServer{resourceManager: resourceManager, httpClient: httpServer.Client()}
	pipeline, err := pipelineServer.CreatePipeline(context.Background(), &api.CreatePipelineRequest{
		Pipeline: &api.Pipeline{
			Url:&api.Url{PipelineUrl: httpServer.URL + "/arguments-parameters.yaml"},
			Name:"argument-parameters",
		}})

	assert.Nil(t, err)
	assert.NotNil(t, pipeline)
	assert.Equal(t, "argument-parameters", pipeline.Name)
	newPipeline, err := resourceManager.GetPipeline(pipeline.Id)
	assert.Nil(t, err)
	assert.NotNil(t, newPipeline)
	var params []api.Parameter
	err = json.Unmarshal([]byte(newPipeline.Parameters), &params)
	assert.Nil(t, err)
	assert.Equal(t, []api.Parameter{{Name: "param1", Value: "hello"}, {Name: "param2"}}, params)
}

func TestCreatePipeline_Tarball(t *testing.T) {
	httpServer := getMockServer(t)
	// Close the server when test finishes
	defer httpServer.Close()

	clientManager := resource.NewFakeClientManagerOrFatal(util.NewFakeTimeForEpoch())
	resourceManager := resource.NewResourceManager(clientManager)

	pipelineServer := PipelineServer{resourceManager: resourceManager, httpClient: httpServer.Client()}
	pipeline, err := pipelineServer.CreatePipeline(context.Background(), &api.CreatePipelineRequest{
		Pipeline: &api.Pipeline{
			Url:&api.Url{PipelineUrl: httpServer.URL + "/arguments_tarball/arguments.tar.gz"},
			Name:"argument-parameters",
		}})

	assert.Nil(t, err)
	assert.NotNil(t, pipeline)
	assert.Equal(t, "argument-parameters", pipeline.Name)
	newPipeline, err := resourceManager.GetPipeline(pipeline.Id)
	assert.Nil(t, err)
	assert.NotNil(t, newPipeline)
	var params []api.Parameter
	err = json.Unmarshal([]byte(newPipeline.Parameters), &params)
	assert.Nil(t, err)
	assert.Equal(t, []api.Parameter{{Name: "param1", Value: "hello"}, {Name: "param2"}}, params)
}

func TestCreatePipeline_InvalidYAML(t *testing.T) {
	httpServer := getMockServer(t)
	// Close the server when test finishes
	defer httpServer.Close()

	clientManager := resource.NewFakeClientManagerOrFatal(util.NewFakeTimeForEpoch())
	resourceManager := resource.NewResourceManager(clientManager)

	pipelineServer := PipelineServer{resourceManager: resourceManager, httpClient: httpServer.Client()}
	_, err := pipelineServer.CreatePipeline(context.Background(), &api.CreatePipelineRequest{
		Pipeline: &api.Pipeline{
			Url:&api.Url{PipelineUrl: httpServer.URL + "/invalid-workflow.yaml"},
			Name:"argument-parameters",
		}})

	assert.NotNil(t, err)
	assert.Equal(t, codes.InvalidArgument, err.(*util.UserError).ExternalStatusCode())
	assert.Contains(t, err.Error(), "Unexpected resource type")
}

func TestCreatePipeline_InvalidURL(t *testing.T) {
	httpServer := getBadMockServer()
	// Close the server when test finishes
	defer httpServer.Close()

	clientManager := resource.NewFakeClientManagerOrFatal(util.NewFakeTimeForEpoch())
	resourceManager := resource.NewResourceManager(clientManager)

	pipelineServer := PipelineServer{resourceManager: resourceManager, httpClient: httpServer.Client()}
	_, err := pipelineServer.CreatePipeline(context.Background(), &api.CreatePipelineRequest{
		Pipeline: &api.Pipeline{
			Url:&api.Url{PipelineUrl: httpServer.URL + "/invalid-workflow.yaml"},
			Name:"argument-parameters",
		}})
	assert.Equal(t, codes.Internal, err.(*util.UserError).ExternalStatusCode())
}

func getMockServer(t *testing.T) *httptest.Server {
	httpServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		file, err := os.Open("test" + req.URL.String())
		assert.Nil(t, err)
		bytes, err := ioutil.ReadAll(file)
		assert.Nil(t, err)

		rw.WriteHeader(http.StatusOK)
		rw.Write(bytes)
	}))
	return httpServer
}

func getBadMockServer() *httptest.Server {
	httpServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(404)
	}))
	return httpServer
}
