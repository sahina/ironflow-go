package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

// SchemaClient provides access to the Ironflow Event Schema Registry API.
type SchemaClient struct {
	client *Client
}

// Schemas returns a SchemaClient for interacting with the event schema registry.
func (c *Client) Schemas() *SchemaClient {
	return &SchemaClient{client: c}
}

func (sc *SchemaClient) rpc() ironflowv1connect.EventSchemaServiceClient {
	return ironflowv1connect.NewEventSchemaServiceClient(sc.client.httpClient, sc.client.serverURL, connect.WithProtoJSON(), connect.WithInterceptors(bearerInterceptor(sc.client.apiKey)))
}

// Register registers a new event schema or a new version.
func (sc *SchemaClient) Register(ctx context.Context, input RegisterSchemaInput) (*EventSchema, error) {
	if input.Version <= 0 || input.Version > math.MaxInt32 {
		return nil, NewError("version must be a positive int32", "INVALID_ARGUMENT", false)
	}
	data, err := json.Marshal(input.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal schema: %w", err)
	}
	// sdkcoverage: POST /ironflow.v1.EventSchemaService/RegisterSchema
	_, err = sc.rpc().RegisterSchema(ctx, connect.NewRequest(&ironflowv1.RegisterSchemaRequest{EventName: input.Name, Version: int32(input.Version), SchemaJson: string(data)}))
	if err != nil {
		return nil, connectError(err)
	}
	return &EventSchema{Name: input.Name, Version: input.Version, Schema: input.Schema}, nil
}

// List returns registered event schemas.
func (sc *SchemaClient) List(ctx context.Context) ([]EventSchema, error) {
	// sdkcoverage: POST /ironflow.v1.EventSchemaService/ListSchemas
	resp, err := sc.rpc().ListSchemas(ctx, connect.NewRequest(&ironflowv1.ListSchemasRequest{}))
	if err != nil {
		return nil, connectError(err)
	}
	result := make([]EventSchema, 0, len(resp.Msg.Schemas))
	for _, schema := range resp.Msg.Schemas {
		result = append(result, *schemaFromProto(schema.EventName, schema.Version, schema.SchemaJson, schema.CreatedAt))
	}
	return result, nil
}

// Get returns the latest registered version of an event schema.
func (sc *SchemaClient) Get(ctx context.Context, name string) (*EventSchema, error) {
	return sc.get(ctx, name, 0)
}

// GetVersion returns a specific registered version.
func (sc *SchemaClient) GetVersion(ctx context.Context, name string, version int) (*EventSchema, error) {
	if version <= 0 || version > math.MaxInt32 {
		return nil, NewError("version must be positive", "INVALID_ARGUMENT", false)
	}
	return sc.get(ctx, name, version)
}

func (sc *SchemaClient) get(ctx context.Context, name string, version int) (*EventSchema, error) {
	// sdkcoverage: POST /ironflow.v1.EventSchemaService/GetSchema
	resp, err := sc.rpc().GetSchema(ctx, connect.NewRequest(&ironflowv1.GetSchemaRequest{EventName: name, Version: int32(version)}))
	if err != nil {
		return nil, connectError(err)
	}
	schema := resp.Msg
	return schemaFromProto(schema.EventName, schema.Version, schema.SchemaJson, schema.CreatedAt), nil
}

func schemaFromProto(name string, version int32, document string, createdAt *timestamppb.Timestamp) *EventSchema {
	result := &EventSchema{Name: name, Version: int(version)}
	// Legacy rows may contain documents that do not parse. Match EventSchema.UnmarshalJSON.
	_ = json.Unmarshal([]byte(document), &result.Schema)
	if createdAt != nil {
		result.CreatedAt = createdAt.AsTime().Format(time.RFC3339Nano)
	}
	return result
}

// Delete removes a specific version of an event schema.
func (sc *SchemaClient) Delete(ctx context.Context, name string, version int) error {
	if version <= 0 || version > math.MaxInt32 {
		return NewError("version must be a positive int32", "INVALID_ARGUMENT", false)
	}
	// sdkcoverage: POST /ironflow.v1.EventSchemaService/DeleteSchema
	_, err := sc.rpc().DeleteSchema(ctx, connect.NewRequest(&ironflowv1.DeleteSchemaRequest{EventName: name, Version: int32(version)}))
	return connectError(err)
}

// TestUpcast tests an upcast transformation between two schema versions.
func (sc *SchemaClient) TestUpcast(ctx context.Context, input TestUpcastInput) (*UpcastResult, error) {
	data, err := json.Marshal(input.Data)
	if err != nil {
		return nil, err
	}
	value := &structpb.Value{}
	if err := protojson.Unmarshal(data, value); err != nil {
		return nil, err
	}
	req := &ironflowv1.TestUpcastRequest{EventName: input.EventName, FromVersion: int32(input.FromVersion), ToVersion: int32(input.ToVersion)}
	if object := value.GetStructValue(); object != nil {
		req.Data = object
	} else {
		req.DataValue = value
	}
	client := sc.rpc()
	// sdkcoverage: POST /ironflow.v1.EventSchemaService/TestUpcast
	resp, err := client.TestUpcast(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, connectError(err)
	}
	var result any
	if resp.Msg.DataValue != nil {
		result = resp.Msg.DataValue.AsInterface()
	} else if resp.Msg.Data != nil {
		result = resp.Msg.Data.AsMap()
	}
	return &UpcastResult{Success: true, Data: result}, nil
}
