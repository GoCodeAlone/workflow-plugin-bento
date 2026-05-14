package internal

import (
	"testing"

	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
)

func TestContractRegistry_NonNil(t *testing.T) {
	p := &bentoPlugin{}
	reg := p.ContractRegistry()
	if reg == nil {
		t.Fatal("ContractRegistry() returned nil")
	}
	if reg.FileDescriptorSet == nil {
		t.Fatal("ContractRegistry().FileDescriptorSet is nil")
	}
	if len(reg.FileDescriptorSet.File) == 0 {
		t.Fatal("ContractRegistry().FileDescriptorSet.File is empty")
	}
	if len(reg.Contracts) == 0 {
		t.Fatal("ContractRegistry().Contracts is empty")
	}
}

func TestContractRegistry_CoversAllModuleTypes(t *testing.T) {
	p := &bentoPlugin{}
	reg := p.ContractRegistry()

	wantModules := []string{"bento.stream", "bento.input", "bento.output", "bento.broker"}
	found := map[string]bool{}
	for _, c := range reg.Contracts {
		if c.Kind == pb.ContractKind_CONTRACT_KIND_MODULE {
			found[c.ModuleType] = true
			if c.Mode != pb.ContractMode_CONTRACT_MODE_STRICT_PROTO {
				t.Errorf("module %s contract mode = %v, want STRICT_PROTO", c.ModuleType, c.Mode)
			}
			if c.ConfigMessage == "" {
				t.Errorf("module %s contract has empty ConfigMessage", c.ModuleType)
			}
		}
	}
	for _, want := range wantModules {
		if !found[want] {
			t.Errorf("no contract descriptor found for module type %s", want)
		}
	}
}

func TestContractRegistry_CoversStepBento(t *testing.T) {
	p := &bentoPlugin{}
	reg := p.ContractRegistry()

	found := false
	for _, c := range reg.Contracts {
		if c.Kind == pb.ContractKind_CONTRACT_KIND_STEP && c.StepType == "step.bento" {
			if c.Mode != pb.ContractMode_CONTRACT_MODE_STRICT_PROTO {
				t.Errorf("step.bento contract mode = %v, want STRICT_PROTO", c.Mode)
			}
			if c.ConfigMessage == "" {
				t.Error("step.bento contract has empty ConfigMessage")
			}
			if c.InputMessage == "" {
				t.Error("step.bento contract has empty InputMessage")
			}
			if c.OutputMessage == "" {
				t.Error("step.bento contract has empty OutputMessage")
			}
			found = true
		}
	}
	if !found {
		t.Error("no contract descriptor found for step type step.bento")
	}
}

func TestContractRegistry_ContractCount(t *testing.T) {
	p := &bentoPlugin{}
	reg := p.ContractRegistry()
	// 4 modules + 1 step = 5 total
	if len(reg.Contracts) != 5 {
		t.Errorf("expected 5 contracts (4 modules + 1 step), got %d", len(reg.Contracts))
	}
}
