package internal

import (
	bentov1 "github.com/GoCodeAlone/workflow-plugin-bento/gen"
	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// ContractRegistry returns the typed contract descriptors for all bento module
// and step types. The workflow engine calls this via the sdk.ContractProvider
// interface to resolve proto message types for strict validation.
func (p *bentoPlugin) ContractRegistry() *pb.ContractRegistry {
	return bentoContractRegistry
}

// bentoContractRegistry declares STRICT_PROTO contracts for all bento module
// and step types. The FileDescriptorSet includes google.protobuf.Struct
// (used in free-form config/data fields) so the engine can resolve all message
// types by full name.
var bentoContractRegistry = &pb.ContractRegistry{
	FileDescriptorSet: &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(structpb.File_google_protobuf_struct_proto),
			protodesc.ToFileDescriptorProto(bentov1.File_bento_proto),
		},
	},
	Contracts: []*pb.ContractDescriptor{
		// ── modules ──────────────────────────────────────────────────────────
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
			ModuleType:    "bento.stream",
			ConfigMessage: bentoProtoPkg + "StreamModuleConfig",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
			ModuleType:    "bento.input",
			ConfigMessage: bentoProtoPkg + "InputModuleConfig",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
			ModuleType:    "bento.output",
			ConfigMessage: bentoProtoPkg + "OutputModuleConfig",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
			ModuleType:    "bento.broker",
			ConfigMessage: bentoProtoPkg + "BrokerModuleConfig",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		// ── steps ────────────────────────────────────────────────────────────
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.bento",
			ConfigMessage: bentoProtoPkg + "ProcessorStepConfig",
			InputMessage:  bentoProtoPkg + "ProcessorStepInput",
			OutputMessage: bentoProtoPkg + "ProcessorStepOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
	},
}

// bentoProtoPkg is the proto package prefix for all bento typed messages.
const bentoProtoPkg = "workflow.plugin.bento.v1."

// Compile-time assertion: bentoPlugin implements sdk.ContractProvider.
var _ interface{ ContractRegistry() *pb.ContractRegistry } = (*bentoPlugin)(nil)
