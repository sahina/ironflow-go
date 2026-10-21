package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

// deploymentSnapshotSchemaVersion is the ExecutionSnapshot schema this SDK's
// generated code knows. The server refuses a newer one rather than store an
// identity it cannot represent (ADR 0081). Raise it with the proto.
const deploymentSnapshotSchemaVersion int32 = 1

// DeploymentClient registers and reads executable deployments (ADR 0081).
type DeploymentClient struct {
	client *Client
}

// Deployments returns a DeploymentClient.
func (c *Client) Deployments() *DeploymentClient {
	return &DeploymentClient{client: c}
}

func (dc *DeploymentClient) rpc() ironflowv1connect.DeploymentServiceClient {
	return ironflowv1connect.NewDeploymentServiceClient(dc.client.httpClient, dc.client.serverURL, connect.WithProtoJSON(), connect.WithInterceptors(dc.client.interceptor()))
}

// RegisterDeploymentInput describes one immutable executable deployment.
type RegisterDeploymentInput struct {
	FunctionID string
	// DeploymentID is chosen by you and never reused: 1-128 ASCII letters,
	// digits, dots, underscores or hyphens, starting with a letter or digit.
	DeploymentID string
	// ArtifactDigest is "sha256:" plus 64 lowercase hex digits.
	ArtifactDigest string
	// Snapshot is the ExecutionSnapshot in its ConnectRPC JSON form, for
	// example {"triggers":[{"event":"order.placed"}],"timeoutMs":30000}.
	// Nil registers an empty snapshot.
	Snapshot json.RawMessage
	Mode     ExecutionMode
	// EndpointURL is required for push mode and must be empty for pull.
	EndpointURL string
}

// Deployment is a registered executable deployment.
type Deployment struct {
	EnvironmentID  string
	FunctionID     string
	DeploymentID   string
	ArtifactDigest string
	// Snapshot is the stored ExecutionSnapshot in its ConnectRPC JSON form.
	Snapshot    json.RawMessage
	Mode        ExecutionMode
	EndpointURL string
	CreatedBy   string
	CreatedAt   time.Time
}

// RegisterDeploymentResult is the stored deployment. Created is false when an
// identical registration already existed.
type RegisterDeploymentResult struct {
	Deployment Deployment
	Created    bool
}

// ListDeploymentsOptions pages ListDeployments.
type ListDeploymentsOptions struct {
	Limit  int32
	Offset int32
}

// FunctionRoutingMode is how a function selects the executable for new runs.
type FunctionRoutingMode string

const (
	// FunctionRoutingLegacy resolves the live function row. Every function starts here.
	FunctionRoutingLegacy FunctionRoutingMode = "legacy"
	// FunctionRoutingAdopting holds dispatch while issued executions drain.
	FunctionRoutingAdopting FunctionRoutingMode = "adopting"
	// FunctionRoutingVersioned pins every new run to the active deployment.
	FunctionRoutingVersioned FunctionRoutingMode = "versioned"
)

// FunctionExecutionRouting reports how a function routes new runs.
type FunctionExecutionRouting struct {
	FunctionID         string
	Mode               FunctionRoutingMode
	ActiveDeploymentID string
	RoutingRevision    int64
	TransitionID       string
	// TransitionStartedAt is nil unless Mode is FunctionRoutingAdopting.
	TransitionStartedAt *time.Time
	UpdatedAt           time.Time
}

// Register stores an immutable deployment. Registering the identical tuple
// again succeeds with Created false; a different artifact, snapshot, mode or
// endpoint under an existing deployment ID fails with already_exists.
func (dc *DeploymentClient) Register(ctx context.Context, input RegisterDeploymentInput) (*RegisterDeploymentResult, error) {
	req := &ironflowv1.RegisterDeploymentRequest{
		FunctionId:            input.FunctionID,
		DeploymentId:          input.DeploymentID,
		ArtifactDigest:        input.ArtifactDigest,
		Mode:                  executionModeToProto(input.Mode),
		EndpointUrl:           input.EndpointURL,
		SnapshotSchemaVersion: deploymentSnapshotSchemaVersion,
	}
	if len(input.Snapshot) > 0 {
		req.Snapshot = &ironflowv1.ExecutionSnapshot{}
		if err := protojson.Unmarshal(input.Snapshot, req.Snapshot); err != nil {
			return nil, NewError(fmt.Sprintf("invalid snapshot: %v", err), "invalid_argument", false)
		}
	}
	// sdkcoverage: POST /ironflow.v1.DeploymentService/RegisterDeployment
	resp, err := dc.rpc().RegisterDeployment(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, connectError(err)
	}
	d, err := deploymentFromProto(resp.Msg.GetDeployment())
	if err != nil {
		return nil, err
	}
	return &RegisterDeploymentResult{Deployment: *d, Created: resp.Msg.GetCreated()}, nil
}

// Get returns one registered deployment.
func (dc *DeploymentClient) Get(ctx context.Context, functionID, deploymentID string) (*Deployment, error) {
	// sdkcoverage: POST /ironflow.v1.DeploymentService/GetDeployment
	resp, err := dc.rpc().GetDeployment(ctx, connect.NewRequest(&ironflowv1.GetDeploymentRequest{FunctionId: functionID, DeploymentId: deploymentID}))
	if err != nil {
		return nil, connectError(err)
	}
	return deploymentFromProto(resp.Msg.GetDeployment())
}

// List returns a function's deployments, newest first.
func (dc *DeploymentClient) List(ctx context.Context, functionID string, opts ListDeploymentsOptions) ([]Deployment, error) {
	// sdkcoverage: POST /ironflow.v1.DeploymentService/ListDeployments
	resp, err := dc.rpc().ListDeployments(ctx, connect.NewRequest(&ironflowv1.ListDeploymentsRequest{FunctionId: functionID, Limit: opts.Limit, Offset: opts.Offset}))
	if err != nil {
		return nil, connectError(err)
	}
	result := make([]Deployment, 0, len(resp.Msg.GetDeployments()))
	for _, pd := range resp.Msg.GetDeployments() {
		d, err := deploymentFromProto(pd)
		if err != nil {
			return nil, err
		}
		result = append(result, *d)
	}
	return result, nil
}

// GetExecutionRouting reports how a function selects the executable for new runs.
func (dc *DeploymentClient) GetExecutionRouting(ctx context.Context, functionID string) (*FunctionExecutionRouting, error) {
	// sdkcoverage: POST /ironflow.v1.DeploymentService/GetFunctionExecutionRouting
	resp, err := dc.rpc().GetFunctionExecutionRouting(ctx, connect.NewRequest(&ironflowv1.GetFunctionExecutionRoutingRequest{FunctionId: functionID}))
	if err != nil {
		return nil, connectError(err)
	}
	m := resp.Msg
	r := &FunctionExecutionRouting{
		FunctionID:         m.GetFunctionId(),
		Mode:               FunctionRoutingMode(strings.ToLower(strings.TrimPrefix(m.GetMode().String(), "FUNCTION_ROUTING_MODE_"))),
		ActiveDeploymentID: m.GetActiveDeploymentId(),
		RoutingRevision:    m.GetRoutingRevision(),
		TransitionID:       m.GetTransitionId(),
	}
	if ts := m.GetTransitionStartedAt(); ts != nil {
		t := ts.AsTime()
		r.TransitionStartedAt = &t
	}
	if ts := m.GetUpdatedAt(); ts != nil {
		r.UpdatedAt = ts.AsTime()
	}
	return r, nil
}

func executionModeToProto(m ExecutionMode) ironflowv1.ExecutionMode {
	switch m {
	case PushMode:
		return ironflowv1.ExecutionMode_EXECUTION_MODE_PUSH
	case PullMode:
		return ironflowv1.ExecutionMode_EXECUTION_MODE_PULL
	default:
		return ironflowv1.ExecutionMode_EXECUTION_MODE_UNSPECIFIED
	}
}

func deploymentFromProto(pd *ironflowv1.Deployment) (*Deployment, error) {
	if pd == nil {
		return nil, NewError("server returned no deployment", "internal", false)
	}
	d := &Deployment{
		EnvironmentID:  pd.GetEnvironmentId(),
		FunctionID:     pd.GetFunctionId(),
		DeploymentID:   pd.GetDeploymentId(),
		ArtifactDigest: pd.GetArtifactDigest(),
		Mode:           ExecutionMode(strings.ToLower(strings.TrimPrefix(pd.GetMode().String(), "EXECUTION_MODE_"))),
		EndpointURL:    pd.GetEndpointUrl(),
		CreatedBy:      pd.GetCreatedBy(),
	}
	if pd.GetSnapshot() != nil {
		raw, err := protojson.Marshal(pd.GetSnapshot())
		if err != nil {
			return nil, fmt.Errorf("encode snapshot: %w", err)
		}
		d.Snapshot = raw
	}
	if ts := pd.GetCreatedAt(); ts != nil {
		d.CreatedAt = ts.AsTime()
	}
	return d, nil
}
